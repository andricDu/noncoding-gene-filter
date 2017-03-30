package org.icgc.dcc.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.broadinstitute.variant.variantcontext.writer.VariantContextWriterFactory;
import org.broadinstitute.variant.vcf.*;
import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.core.resolver.ReferenceGenomeResolver;
import org.icgc.dcc.release.job.annotate.model.SecondaryEntity;
import org.icgc.dcc.release.job.annotate.resolver.JavaResolver;
import org.icgc.dcc.release.job.annotate.resolver.SnpEffDatabaseResolver;
import org.icgc.dcc.release.job.annotate.resolver.SnpEffJarResolver;
import org.icgc.dcc.release.job.annotate.snpeff.SnpEffLogHandler;
import org.icgc.dcc.release.job.annotate.snpeff.SnpEffPredictor;
import org.icgc.dcc.release.job.annotate.snpeff.SnpEffProcess;
import org.icgc.dcc.release.job.annotate.snpeff.SnpEffResultHandler;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;


import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.regex.Pattern.compile;

@Slf4j
@Component
public class Filter implements CommandLineRunner {

  /**
   * Configuration
   */
  private SnpEffProperties properties = new SnpEffProperties();

  /**
   * Constants
   */
  private static final int PREDICTION_TIMEOUT_MINUTES = 5;

  /**
   * State.
   */
  private BlockingQueue<Boolean> queue = new ArrayBlockingQueue<Boolean>(100);
  private SnpEffProcess process;
  private ExecutorService executor = Executors.newFixedThreadPool(2);
  private PrintStream stream;

  @Override
  @SneakyThrows
  public void run(String... args) {
    log.info("*** *** Starting Filtering *** ***");
    setProperties();

    process = new SnpEffProcess(this.resolveJar(), this.resolveJava(), this.resolveDataDir(), this.properties.getDatabaseVersion());
    stream = new PrintStream(process.getOutputStream(), false, UTF_8.name());

    // Start handler threads
    executor.execute(new ResultsHandler(process.getInputStream(), queue, properties
        .getGeneBuildVersion()));
    executor.execute(new SnpEffLogHandler(process.getErrorStream()));
    initializeSnpEff();

    val lines = Files.lines(Paths.get(args[0]));
    lines.forEach(this::predict);
  }

  @SneakyThrows
  public Boolean predict(String line) {
    stream.println(line);
    stream.flush();

    val predictions = queue.poll(PREDICTION_TIMEOUT_MINUTES, MINUTES);
    val timeout = predictions == null;
    if (timeout) {
      checkState(false,
          "Timeout after waiting %s min for next prediction from SnpEff process. Exit code = %s",
          PREDICTION_TIMEOUT_MINUTES,
          process.isAlive() ? process.exitValue() : "<still running!>");
    }

    log.info(predictions.toString());
    return predictions;
  }

  @SneakyThrows
  private void initializeSnpEff() {
    log.warn("Initializing SnpEff...");

    // VariantContextWriterFactory requires a non-null FILE. Create any and delete it on exit
    val prefix = SnpEffPredictor.class.getName();
    val file = File.createTempFile(prefix, null);
    file.deleteOnExit();

    val writer = VariantContextWriterFactory.create(file, stream, null);
    writer.writeHeader(createAnnotatedVCFHeader());
    stream.flush();

    deleteTempFile(file);
  }

  private void setProperties() {
    properties.setDatabaseVersion("3.6c-GRCh37.75");
    properties.setGeneBuildVersion("75");
    properties.setMaxFileSizeMb(512);
    properties.setReferenceGenomeVersion("GRCh37.75.v1");
    properties.setResourceDir(new File("/tmp/dcc-release"));
    properties.setResourceUrl("https://artifacts.oicr.on.ca/artifactory/simple/dcc-dependencies/org/icgc/dcc");
    properties.setVersion("3.6c");
  }

  private static VCFHeader createAnnotatedVCFHeader() {
    return new VCFHeader(
        ImmutableSet.of(new VCFHeaderLine("PEDIGREE", "<Derived=Patient_01_Somatic,Original=Patient_01_Germline>")),
        ImmutableList.of("Patient_01_Germline", "Patient_01_Somatic"));
  }

  private void deleteTempFile(File orifinalFile) {
    val tmpFile = new File(orifinalFile.getAbsolutePath() + ".idx");
    tmpFile.deleteOnExit();
  }

  private File resolveJava() {
    JavaResolver resolver = new JavaResolver();
    return resolver.resolve();
  }

  private File resolveJar() {
    SnpEffJarResolver resolver = new SnpEffJarResolver(this.properties.getResourceDir(), this.properties.getVersion());
    return (File)resolver.resolve();
  }

  private File resolveDataDir() {
    SnpEffDatabaseResolver resolver = new SnpEffDatabaseResolver(this.properties.getResourceDir(), this.properties.getResourceUrl(), this.properties.getDatabaseVersion());
    return (File)resolver.resolve();
  }

  private File resolveReferenceGenome() {
    ReferenceGenomeResolver resolver = new ReferenceGenomeResolver(this.properties.getResourceDir(), this.properties.getResourceUrl(), this.properties.getReferenceGenomeVersion());
    return (File)resolver.resolve();
  }

}

package org.icgc.dcc.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import org.broadinstitute.variant.vcf.*;
import org.icgc.dcc.release.job.annotate.converter.SnpEffVCFToICGCConverter;
import org.icgc.dcc.release.job.annotate.model.SecondaryEntity;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.regex.Pattern.compile;

/**
 * Created by dandric on 3/29/17.
 */
public class ResultsHandler implements Runnable {
  private static final Pattern SKIP_ANNOTATION_PATTERN = compile("^#|Reading cancer samples pedigree from VCF header");

  /**
   * Dependencies.
   */
  @NonNull
  private final InputStream input;
  private final VCFCodec decoder = createDecoder();

  /**
   * State.
   */
  @NonNull
  private final BlockingQueue<String> queue;
  private final SnpEffVCFToICGCConverter converter;

  public ResultsHandler(@NonNull InputStream input, @NonNull BlockingQueue<String> queue,
                        @NonNull String geneBuildVersion) {
    this.input = input;
    this.queue = queue;
    this.converter = new SnpEffVCFToICGCConverter(geneBuildVersion);
  }

  @Override
  @SneakyThrows
  public void run() {
    val reader = new BufferedReader(new InputStreamReader(input, UTF_8));
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (isSkipLine(line)) {
        continue;
      }

      queue.put(line);
    }
  }

  private static boolean isSkipLine(String line) {
    val matcher = SKIP_ANNOTATION_PATTERN.matcher(line);

    return matcher.find();
  }

  private VCFCodec createDecoder() {
    val decoder = new VCFCodec();
    val vcfHeader = createVCFHeader();
    decoder.setVCFHeader(vcfHeader, VCFHeaderVersion.VCF4_1);

    return decoder;
  }

  private static VCFHeader createVCFHeader() {
    return new VCFHeader(
        ImmutableSet.of(
            new VCFInfoHeaderLine(
                "<ID=EFF,Number=.,Type=String,Description=\"Predicted effects for this variant.Format: 'Effect ( Effect_Impact | Functional_Class | Codon_Change | Amino_Acid_Change| Amino_Acid_length | Gene_Name | Transcript_BioType | Gene_Coding | Transcript_ID | Exon_Rank  | Genotype_Number [ | ERRORS | WARNINGS ] )' \">",
                VCFHeaderVersion.VCF4_1),
            new VCFFormatHeaderLine(
                "<ID=GT,Number=1,Type=String,Description=\"Genotype\">",
                VCFHeaderVersion.VCF4_1)),
        ImmutableList.of(
            "Patient_01_Germline", "Patient_01_Somatic"));
  }

}

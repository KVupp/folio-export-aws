package org.folio.folio_export_aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class App {
  static AmazonS3 s3Client;
  static Regions clientRegion = Regions.US_EAST_1;
  static String bucketName = "test-aws-export-vk";
  static String stringObjKeyName = "testFile1.mrc";
  static String keyName = "testFileMultiPart.mrc";

  public static void main(String[] args) throws IOException {

    try {
      // This code expects that you have AWS credentials set up per:
      // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
      // uses credential Chain to fetch credentials
      s3Client = AmazonS3ClientBuilder.standard()
        .withRegion(clientRegion)
        .build();

      App app = new App();
      if (!s3Client.doesBucketExist(bucketName)) {
        app.createBucket();
      }
      app.uploadString();
      app.getPresignedURL(stringObjKeyName);

      app.saveFilesUsingTransferManager();

    } catch (AmazonServiceException e) {
      // The call was transmitted successfully, but Amazon S3 couldn't process
      // it, so it returned an error response.
      e.printStackTrace();
    } catch (SdkClientException e) {
      // Amazon S3 couldn't be contacted for a response, or the client
      // couldn't parse the response from Amazon S3.
      e.printStackTrace();
    }
  }

  public void createBucket() {
    s3Client.createBucket(bucketName);

  }

  /**
   * Creates a file with the given String
   */
  public void uploadString() {
    // Upload a text string as a new object.
    s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object123");

  }

  /**
   * Fetches a presigned URL for the given object in the bucket. The URL expiry is set to 1 hr
   *
   * @param keyName
   * @return
   */
  public String getPresignedURL(String keyName) {
    java.util.Date expiration = new java.util.Date();
    long expTimeMillis = expiration.getTime();
    expTimeMillis += 1000 * 60 * 60;
    expiration.setTime(expTimeMillis);

    // Generate the presigned URL.
    System.out.println("Generating pre-signed URL.");
    GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, keyName)
      .withMethod(HttpMethod.GET)
      .withExpiration(expiration);
    URL url = s3Client.generatePresignedUrl(generatePresignedUrlRequest);

    System.out.println("Pre-Signed URL: " + url.toString());
    return url.toString();
  }

  /**
   * Save the files in a given folder into S3. They will be created in a subfolder with the name specified in prefix
   */
  public void saveFilesUsingTransferManager() {
    TransferManager xfer_mgr = TransferManagerBuilder.standard()
      .withS3Client(s3Client)
      .build();
    try {
      System.err.println("uploading files");
      MultipleFileUpload xfer = xfer_mgr.uploadDirectory(bucketName, "kvMP",
          new File("/Users/kvuppala/git/folio-export-aws/fileExport"), false);

      xfer.waitForCompletion();
    } catch (AmazonServiceException e) {
      System.err.println(e.getErrorMessage());
      System.exit(1);
    } catch (AmazonClientException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    xfer_mgr.shutdownNow();

  }

  /**
   * Multi part upload is possible only when each part size is above >=5MB So the below method throws an error
   */
  public void multipartUpload() {
    List<PartETag> partETags = new ArrayList<PartETag>();
    InputStream is;
    int i = 1;

    // Initiate the multipart upload.
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

    List<String> marcRecords = new ArrayList<String>();
    marcRecords.add(
        "00474nam  22001455  4500003000800000008004100008020001800049040002100067100002400088245007900112260003600191440004400227980003600271981002100307NhCcYBP071116s2008    nyua     b    000 0 eng d  a9780820488660  aNhCcYBPcNhCcYBP1 aBruns, Axel,d1970-00aBlogs, Wikipedia, Second Life, and beyond :bfrom production to produsage.  aNew York :bPeter Lang,cc2008. 0aDigital formations,x1526-3169 ;vv. 45  a080313b3495d000e3495f362718  aya26Mbzybpwcdd00416nam  22001335  4500003000800000008004100008020001800049040002100067100004300088245005800131260003600189980003600225981002100261NhCcYBP071030s2008    nyua     b    001 0 eng d  a9781433102172  aNhCcYBPcNhCcYBP1 aHoward, V. A.q(Vernon Alfred),d1935-00aCharm and speed :bvirtuosity in the performing arts.  aNew York :bPeter Lang,cc2008.  a080313b2995d000e2995f362718  aya26mbzybpwcdd00407nam  22001335  4500003000800000008004100008020001800049040002100067245007500088260003400163700001800197980003700215981002100252NhCcYBP070813s2008    nyua          001 0 eng d  a9783540681359  aNhCcYBPcNhCcYBP00aDemographic change in Germany :bthe economic and fiscal consequences.  aNew York :bSpringer,cc2008.1 aHamm, Ingrid.  a080313b10900d000e8884f362718  aya26Mbzybpwcdd00471nam  22001335  4500003000800000008004100008020001800049040002100067245009000088260006200178700004000240980003600280981002100316NhCcYBP070920s2008    ne a     b    001 0 eng d  a9789027253057  aNhCcYBPcNhCcYBP00aDevelopmental psycholinguistics :bon-line methods in children's language processing.  aAmsterdam ;aPhiladelphia :bJohn Benjamins Pub.,cc2008.1 aSekerina, I. A.q(Irina A.),d1961-  a080313b4495d000e4495f362718  aya26Mbzybpwcdd00397nam  22001335  4500003000800000008004100008020001800049040002100067100002800088245005600116260003400172980003600206981002100242NhCcYBP070803s2008    nyua     b    001 0 eng d  a9780387726601  aNhCcYBPcNhCcYBP1 aAdams, Peter J.,d1956-00aFragmented intimacy :baddiction in a social world.  aNew York :bSpringer,cc2008.  a080313b5995d000e4886f362718  aya26Mbzybpwcdd00386nam  22001335  4500003000800000008004100008020001800049040002100067100002600088245003400114260004500148980003800193981002100231NhCcYBP070917t20082007ne ab    b    001 0 eng d  a9789004157446  aNhCcYBPcNhCcYBP1 aCollins, Billie Jean.04aThe Hittites and their world.  aLeiden ;aBoston :bBrill,c2008, c2007.  a080313b13700d000e11166f362718  aya26Mbzybpwcdd00515nam  22001455  4500003000800000008004100008020001800049040002100067245008000088260005300168440005400221700003500275980003800310981002100348NhCcYBP070612s2007    gw a     b    001 0 eng d  a9783110195590  aNhCcYBPcNhCcYBP00aLearning indigenous languages :bchild language acquisition in Mesoamerica.  aBerlin ;aNew York :bMouton de Gruyter,cc2007. 0aStudies on language acquisition,x1861-4248 ;v331 aPfeiler, Barbara Blaha,d1952-  a080313b10100d000e10100f362718  aya26Mbzybpwcdd00417nam  22001335  4500003000800000008004100008020001800049040002100067245007200088260004800160700001800208980003600226981002100262NhCcYBP071023s2007    ne a     bq   000 0 eng d  a9789042022539  aNhCcYBPcNhCcYBP00aMonsters and the monstrous :bmyths and metaphors of enduring evil.  aAmsterdam ;aNew York, NY :bRodopi,c2007.1 aScott, Niall.  a080313b6815d000e5554f362718  aya26Mbzybpwcdd00423nam  22001335  4500003000800000008004100008020001800049040002100067245007900088260003300167700003000200980003800230981002100268NhCcYBP070815s2007    nyua     b    001 0 eng d  a9780387360331  aNhCcYBPcNhCcYBP00aNew computational paradigms :bchanging conceptions of what is computable.  aNew York :bSpringer,c2008.1 aCooper, S. B.q(S. Barry)  a080313b13900d000e11329f362718  aya26Mbzybpwcdd00474nam  22001455  4500003000800000008004100008020001800049040002100067100002100088245006400109260003700173440005900210980003800269981002100307NhCcYBP080307s2008    ne                  eng d  a9781402066511  aNhCcYBPcNhCcYBP1 aHansson, Mats G.04aThe private sphere :ban emotional territory and its agent.  a[Dordrecht] :bSpringer,cc2008. 0aPhilosophical studies in contemporary culture ;vv. 15  a080313b12900d000e10514f362718  aya26Mbzybpwcdd00494nam  22001455  4500003000800000008004100008020001800049040002100067100002500088245007500113260003600188440006700224980003600291981002100327NhCcYBP070129s2008    nyu      b    001 0 eng d  a9781433101779  aNhCcYBPcNhCcYBP1 aHoechsmann, Michael.00aReading youth writing :bnew literacies, cultural studies & education.  aNew York :bPeter Lang,cc2008. 0aNew literacies and digital epistemologies,x1523-9543 ;vv. 26  a080313b2995d000e2995f362718  aya26Mbzybpwcdd00499nam  22001455  4500003000800000008004100008020001800049040002100067245003900088260004300127440009500170700003100265980003600296981002100332NhCcYBP080311s2007    ne                  eng d  a9789042023284  aNhCcYBPcNhCcYBP00aStories and portraits of the self.  aAmsterdam ;aNew York :bRodopi,c2007 0aInternationale Forschungen zur allgemeinen und vergleichenden Literaturwissenschaft ;v1151 aBuescu, Helena Carvalhäao.  a080313b9715d000e7918f362718  aya26Mbzybpwcdd00409nam  22001335  4500003000800000008004100008020001800049040002100067245006800088260003400156700002600190980003800216981002100254NhCcYBP080310s2008    nyu                 eng d  a9780387745077  aNhCcYBPcNhCcYBP00aViolence in Europe :bhistorical and contemporary perspectives.  aNew York :bSpringer,cc2008.1 aBody-Gendrot, Sophie.  a080313b15900d000e12959f362718  aya26Mbzybpwcdd");
    marcRecords.add(
        "00474nam  22001455  4500003000800000008004100008020001800049040002100067100002400088245007900112260003600191440004400227980003600271981002100307NhCcYBP071116s2008    nyua     b    000 0 eng d  a9780820488660  aNhCcYBPcNhCcYBP1 aBruns, Axel,d1970-00aBlogs, Wikipedia, Second Life, and beyond :bfrom production to produsage.  aNew York :bPeter Lang,cc2008. 0aDigital formations,x1526-3169 ;vv. 45  a080313b3495d000e3495f362718  aya26Mbzybpwcdd00416nam  22001335  4500003000800000008004100008020001800049040002100067100004300088245005800131260003600189980003600225981002100261NhCcYBP071030s2008    nyua     b    001 0 eng d  a9781433102172  aNhCcYBPcNhCcYBP1 aHoward, V. A.q(Vernon Alfred),d1935-00aCharm and speed :bvirtuosity in the performing arts.  aNew York :bPeter Lang,cc2008.  a080313b2995d000e2995f362718  aya26mbzybpwcdd00407nam  22001335");
    marcRecords.add(
        "4500003000800000008004100008020001800049040002100067245007500088260003400163700001800197980003700215981002100252NhCcYBP070813s2008    nyua          001 0 eng d  a9783540681359  aNhCcYBPcNhCcYBP00aDemographic change in Germany :bthe economic and fiscal consequences.  aNew York :bSpringer,cc2008.1 aHamm, Ingrid.  a080313b10900d000e8884f362718  aya26Mbzybpwcdd00471nam  22001335  ");
    marcRecords.add(
        "4500003000800000008004100008020001800049040002100067245009000088260006200178700004000240980003600280981002100316NhCcYBP070920s2008    ne a     b    001 0 eng d  a9789027253057  aNhCcYBPcNhCcYBP00aDevelopmental psycholinguistics :bon-line methods in children's language processing.  aAmsterdam ;aPhiladelphia :bJohn Benjamins Pub.,cc2008.1 aSekerina, I. A.q(Irina A.),d1961-  a080313b4495d000e4495f362718  aya26Mbzybpwcdd00397nam  22001335  ");
    marcRecords.add(
        "4500003000800000008004100008020001800049040002100067100002800088245005600116260003400172980003600206981002100242NhCcYBP070803s2008    nyua     b    001 0 eng d  a9780387726601  aNhCcYBPcNhCcYBP1 aAdams, Peter J.,d1956-00aFragmented intimacy :baddiction in a social world.  aNew York :bSpringer,cc2008.  a080313b5995d000e4886f362718  aya26Mbzybpwcdd00386nam  22001335 ");

    for (String s : marcRecords) {
      long partSize = s.length();
      is = new ByteArrayInputStream(s.getBytes());

      UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucketName)
        .withKey(keyName)
        .withPartNumber(i++)
        .withUploadId(initResponse.getUploadId())
        .withInputStream(is)
        .withPartSize(partSize);
      UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
      partETags.add(uploadResult.getPartETag());

    }
    CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, keyName, initResponse.getUploadId(),
        partETags);
    s3Client.completeMultipartUpload(compRequest);
  }
}

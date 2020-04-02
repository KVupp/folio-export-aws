package org.folio.folio_export_aws;

import io.minio.MinioClient;
import io.minio.PutObjectOptions;
import io.minio.errors.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import org.xmlpull.v1.XmlPullParserException;

public class MinIo {
  public static void main(String[] args)
      throws IOException, NoSuchAlgorithmException, InvalidKeyException, XmlPullParserException {
    String bucketName = "test";
    try {
      /* play.min.io for test and development. */
      MinioClient minioClient =
          new MinioClient(
              "http://127.0.0.1:9001",
              "minioadmin",
              "minioadmin");

      /* Amazon S3: */
      // MinioClient minioClient = new MinioClient("https://s3.amazonaws.com", "YOUR-ACCESSKEYID",
      //                                           "YOUR-SECRETACCESSKEY");

      // Create some content for the object.
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < 1000; i++) {
        builder.append(
            "Testing minio using a docker on my local MAC ");
        builder.append("(29 letters)\n");

        builder.append("---\n");
      }

      // Create a InputStream for object upload.
      ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));

      // Create object 'my-objectname' in 'my-bucketname' with content from the input stream.
      minioClient.putObject(
          bucketName, "testkv", bais, new PutObjectOptions(bais.available(), -1));
      bais.close();
      System.out.println("my-objectname is uploaded successfully");

      String URL = getPreSignedURL(minioClient, bucketName,  "testkv");
      System.out.println("PreSigned URL "+ URL);
    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
    }
  }


  public static String getPreSignedURL(MinioClient minioClient,String bucketName,String keyName) {
    try {
      return minioClient.presignedGetObject(bucketName, keyName, 60 * 60 * 24);
    } catch (InvalidKeyException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InvalidBucketNameException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InsufficientDataException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (XmlParserException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ErrorResponseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InternalException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InvalidExpiresRangeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InvalidResponseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }
}
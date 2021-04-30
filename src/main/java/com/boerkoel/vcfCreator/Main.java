package com.boerkoel.vcfCreator;

import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import net.minidev.json.*;
import net.minidev.json.parser.*;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
 
public class Main {

  public static final String SERVER = "https://grch37.rest.ensembl.org";
  public static final JSONParser PARSER = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
  
  public static int requestCount = 0;
  public static long lastRequestTime = System.currentTimeMillis();

  public static FileOutputStream errOutStream = null;
 
  public static void main(String[] args) throws Exception {
    try {
      File errOut = new File("/Users/pierreboerkoel/Programming/vcf_getter/vcf_output/error_output.txt");
      errOutStream = new FileOutputStream(errOut);
    } catch (Exception e) {
      System.out.println("Failed to create error file for project: " + e.toString());
    }

    String filePath = "/Users/pierreboerkoel/Programming/vcf_getter/RefSeqGenes/testing2.csv";
    BufferedReader inputReader = getLineReader(filePath);
    inputReader.readLine(); // Discard the first line - contains column names
    String line = null;

    while ((line = inputReader.readLine()) != null) {
      String subjectId = getSubjectID(line);
      ArrayList<String> requests = formRequests(line);
      if (requests.size() < 1) {
        continue;
      }
      JSONArray results = new JSONArray();
      
      for (String request: requests) {
        try {
          results.add(getVcfString(request));
        } catch (Exception e) {
          String errTxt = "Request failed: " + request + " " + e.toString();
          logError(errTxt);
          System.out.println(errTxt + "\n");
        }
      }
      writeVcfFile(subjectId, results);
    }
    inputReader.close();
    errOutStream.close();
  }

  // Get the Subject ID
  public static String getSubjectID(String data) {
    if (data == null || data.isEmpty()) {
      return "";
    }
    String splitBy = ",";
    String[] lineData = data.split(splitBy);
    return lineData[0];
  }

  // log errors to error_output.txt
  public static void logError(String error) {
    try {
      error = error + "\n";
      errOutStream.write(error.getBytes());
    } catch (IOException e) {
      System.out.println("Failed to write to the error file");
      e.printStackTrace();
    }
  }

  // Write the vcf file
  public static void writeVcfFile(String subjectID, JSONArray subjectResults) {
    try {
      File vcfOut = new File("/Users/pierreboerkoel/Programming/vcf_getter/vcf_output/" + subjectID + ".vcf");
      FileOutputStream fop = new FileOutputStream(vcfOut);

      if(vcfOut.exists()) {
        String str="##fileformat=VCFv4.3\n" + 
          "##Variant Recoder\n" +
          "CHROM\t" + "POS\t" + "ID\t" + "REF\t" + "ALT\t" + "QUAL\t" + "FILTER\t" + "INFO\n";

        for (int i = 0; i < subjectResults.size(); i++) {
          try {
            JSONArray resultsJsonArray = (JSONArray) subjectResults.get(i);
            JSONObject allResultsJsonObject = (JSONObject) resultsJsonArray.get(0); // Only one object is present in the JSON Array
            Iterator<String> keyIterator = allResultsJsonObject.keySet().iterator();

            while(keyIterator.hasNext()) {
              String key = keyIterator.next();
              if (allResultsJsonObject.get(key) instanceof JSONObject) {
                JSONObject resultObject = (JSONObject) allResultsJsonObject.get(key);
                JSONArray resultVcfArray = (JSONArray) resultObject.get("vcf_string");
                if (resultVcfArray != null) {
                  for (int j = 0; j < resultVcfArray.size(); j++) {
                    String vcfValue = (String) resultVcfArray.get(j);
                    String[] vcfValues = vcfValue.split("-"); // API reports with a '-' separating fields
                    str = str + vcfValues[0] + "\t" + vcfValues[1] + "\t" + subjectID + "\t" + vcfValues[2] + "\t" +vcfValues[3] + "\t" + "30\t" + ".\t" + ".\n";
                  }  
                } else {
                  String errTxt = "No vcf_string found for subject: " + subjectID;
                  logError(errTxt);
                  System.out.println(errTxt);
                  continue;
                }  
              }
            }
            
          } catch (Exception e) {
            String errTxt = "Failed to write vcf data for subject: " + subjectID + " " + e.toString() + " ";
            logError(errTxt);
            System.out.println(errTxt + "\n");
            e.printStackTrace();
            continue;
          }
        }
        fop.write(str.getBytes());

        //close the output stream and buffer reader 
        fop.flush();
        fop.close();
      } else {
        String errTxt = "Failed to create a vcf file for subject: " + subjectID;
        logError(errTxt);
        System.out.println(errTxt + "\n");
      }
    } catch (Exception e) {
      String errTxt = "Failed to create a vcf file for subject: " + subjectID + " " + e.toString();
      logError(errTxt);
      System.out.println(errTxt + "\n");
    }
  }

  // Form the HTTP request strings
  public static ArrayList<String> formRequests(String data) {
    ArrayList<String> requests = new ArrayList<>();
    if (data == null || data.isEmpty()) {
      return requests;
    }

    String splitBy = ",";
    String[] lineData = data.split(splitBy);

    int lineOffset = 0;
    int transcriptIndex = 1;
    int mutationIndex = 2;

    while (lineData.length > lineOffset + mutationIndex) {
      String transcript = lineData[transcriptIndex + lineOffset];
      String mutation = lineData[mutationIndex + lineOffset];
      String request = transcript + ":" + mutation + "?";
      requests.add(request);
      lineOffset = lineOffset + 2;
    }
    return requests;
  }

  // Return a BufferedReader of the given file
  public static BufferedReader getLineReader(String filePath) {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(filePath));
    } catch (IOException io) {
      System.out.println(io.toString() + "\n");
    }
    if (br == null) {
      throw new RuntimeException("Could not initialize the BufferedReader for file: " + filePath);
    }
    return br;
  }


  public static JSONArray getVcfString(String request) throws ParseException, MalformedURLException, IOException, InterruptedException {
    // String id = getGeneID(request);
    String formattedRequest = "/variant_recoder/human/" + request + "fields=vcf_string";
    return (JSONArray) getJSON(formattedRequest);
  }


  public static Object getJSON(String endpoint) throws ParseException, MalformedURLException, IOException, InterruptedException {
    String jsonString = getContent(endpoint);
    return PARSER.parse(jsonString);
  }


  public static String getContent(String endpoint) throws MalformedURLException, IOException, InterruptedException { 
    if(requestCount == 15) { // check every 15
      long currentTime = System.currentTimeMillis();
      long diff = currentTime - lastRequestTime;
      //if less than a second then sleep for the remainder of the second
      if(diff < 1000) {
        Thread.sleep(1000 - diff);
      }
      //reset
      lastRequestTime = System.currentTimeMillis();
      requestCount = 0;
    }
    
    URL url = new URL(SERVER+endpoint);
    URLConnection connection = url.openConnection();
    HttpURLConnection httpConnection = (HttpURLConnection)connection;
    httpConnection.setRequestProperty("Content-Type", "application/json");
    requestCount++;

    InputStream response = httpConnection.getInputStream();
    int responseCode = httpConnection.getResponseCode();

    if(responseCode != 200) {
      if(responseCode == 429 && httpConnection.getHeaderField("Retry-After") != null) {
        double sleepFloatingPoint = Double.valueOf(httpConnection.getHeaderField("Retry-After"));
        double sleepMillis = 1000 * sleepFloatingPoint;
        Thread.sleep((long)sleepMillis);
        return getContent(endpoint);
      }
      throw new RuntimeException("Response code was not 200. Detected response was "+responseCode);
    }

    String output;
    Reader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(response, "UTF-8"));
      StringBuilder builder = new StringBuilder();
      char[] buffer = new char[8192];
      int read;
      while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
        builder.append(buffer, 0, read);
      }
      output = builder.toString();
    } 
    finally {
      if (reader != null) {
        try {
          reader.close(); 
        } 
        catch (IOException logOrIgnore) {
          logOrIgnore.printStackTrace();
        }
      }
    }
  
    return output;
  }
}

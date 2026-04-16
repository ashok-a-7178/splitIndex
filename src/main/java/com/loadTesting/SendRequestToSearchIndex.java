package com.loadTesting;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;

public class SendRequestToSearchIndex
{
    public static void main(String[] args) throws Exception
    {
        sendChangeObject("",false);
    }
    private static void sendChangeObject(String parameterString, boolean isSyncChange) throws Exception
    {
        HttpURLConnection httpConnection = null;
        try
        {
            String parameterStringForLog = parameterString;
            long start = System.currentTimeMillis();
            String urlStr = "http://localhost:8080/searchtestchangecollector";
            URL url = new URL(urlStr);
            System.out.println("URL for sending change is " + urlStr + " \n parameter string for change is" + parameterString);

            //parameterString = parameterString + "&iscsignature=ZohoSearch-1776348447204-12c6e970b2d28946a3db14e52b19908b476b15a34ad3c3584b50e8f1bde602843235ae7aa205a74461d8df793c8901a49f5c4370c8f091f1c17e52effd4fac48";
            parameterString = "service=newimplone&version=v1&bulkChgArr=W3siQ0hBTkdFX1RZUEUiOiIxIiwiRU5USVRZX0lEIjoiMSIsIkNIQU5HRV9EQVRBIjp7IlNPTFVUSU9OX05PVDEiOiIxIiwiU09MVVRJT05fTk9UMiI6IjEiLCJTT0xVVElPTl9OT1QzIjoiMSIsIlNPTFVUSU9OX05PVDQiOiIxIiwiU09MVVRJT05fTk9UNSI6IjEifSwiTk9USUZZX0lEIjotMSwiWlNPSUQiOiI4Mzg0ODM4NiIsIk1PRFVMRV9JRCI6IjEifSx7IkNIQU5HRV9UWVBFIjoiMSIsIkVOVElUWV9JRCI6IjIiLCJDSEFOR0VfREFUQSI6eyJTT0xVVElPTl9OT1QxIjoiMiIsIlNPTFVUSU9OX05PVDIiOiIyIiwiU09MVVRJT05fTk9UMyI6IjIiLCJTT0xVVElPTl9OT1Q0IjoiMiIsIlNPTFVUSU9OX05PVDUiOiIyIn0sIk5PVElGWV9JRCI6LTEsIlpTT0lEIjoiODM4NDgzODYiLCJNT0RVTEVfSUQiOiIxIn1d&iscsignature=ZohoSearch-1776348447204-12c6e970b2d28946a3db14e52b19908b476b15a34ad3c3584b50e8f1bde602843235ae7aa205a74461d8df793c8901a49f5c4370c8f091f1c17e52effd4fac48";

            httpConnection = (HttpURLConnection) url.openConnection();
            byte[] bytes = parameterString.getBytes();
            httpConnection.setConnectTimeout(3000);
            httpConnection.setReadTimeout(10000);
            httpConnection.setRequestMethod("POST");
            httpConnection.setRequestProperty("Content-Length", "" + Integer.toString(bytes.length));
            httpConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);

            DataOutputStream printout = null;
            try {
                printout = new DataOutputStream(new BufferedOutputStream(httpConnection.getOutputStream()));
                printout.write(bytes);
                printout.flush();
                printout.close();
            } finally {
                if (printout != null) {
                    printout.close();
                }
            }

            int responseCode = httpConnection.getResponseCode();
            System.out.println("Response code obtained for pushing a change is  " + responseCode + " and time taken is " + (System.currentTimeMillis() - start));
            if (responseCode == 200) {
                System.out.println("Sending the change was successful with status code " + responseCode + " for the change URL : " + url.getPath());
            } else
            {
                System.out.println("Exception while pushing a change. Response code received is  " + responseCode + " URL is " + url);
            }
        } catch (ProtocolException pe) {
            System.out.println("ProtocolException while sending changes to the search grid: " + pe.getMessage());
            pe.printStackTrace();
            throw pe;
        } catch (IOException ioe) {
            System.out.println("IOException while sending changes to the search grid: " + ioe.getMessage());
            ioe.printStackTrace();
            throw ioe;
        } catch (Exception ex) {
            System.out.println("Exception while sending change to search grid: " + ex.getMessage());
            ex.printStackTrace();
            throw ex;
        } finally {
            if (httpConnection != null) {
                httpConnection.disconnect();
            }
        }

    }

}

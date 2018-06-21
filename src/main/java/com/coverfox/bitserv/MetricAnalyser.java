package com.coverfox.bitserv;


// use event bus instead [LATER]

public class MetricAnalyser {

  public static Integer buffering = 0;
  public static Integer dispatchCalls = 0;

  public static Integer networkCalls = 0;
  public static Integer networkCallsRepeat = 0;
  
  public static Integer bqErrors = 0;
  public static Integer bqErrorsRepeat = 0;
  
  public static Integer bQFailures = 0;
  public static Integer bQFailuresRepeat = 0;
  
  public static boolean debug = false;

  public static void setDebugMode(boolean debugValue){
    debug = debugValue;
  }

  public static void dispatchCall(Integer dispatchSize, String identifier){
    if (debug) System.out.println("[DISPATCH] (" + identifier +") : "+ Integer.toString(dispatchSize));
    dispatchCalls++;
  }
  // print time taken for the network call
  public static void networkCall(Integer repeatIndex){
    if (repeatIndex == 2) {
      networkCallsRepeat++;
      if(debug) System.out.println("[NETWORK] 2");
    }
    else networkCall();
  }
  public static void networkCall(){
    if(debug) System.out.println("[NETWORK]");
    networkCalls++;
  }
  public static void buffering(){
    buffering++;
  }
  public static void BigqueryError(){
    if(debug) System.out.println("[BIGQUERY ERROR]");
    bqErrors++;
  }
  public static void BigqueryError(Integer repeatIndex){
    if (repeatIndex == 2) {
      bqErrorsRepeat++;
      System.out.println("[BIGQUERY ERROR] 2");
    }
    else BigqueryError();
  }
  public static void BigqueryFailure(){
    if(debug) System.out.println("[BIGQUERY FAILURE]");
    bQFailures++;
  }
  public static void BigqueryFailure(Integer repeatIndex){
    if (repeatIndex == 2) {
      bQFailuresRepeat++;
      if(debug) System.out.println("[BIGQUERY FAILURE] 2");
    }
    else BigqueryFailure();
  }
  public static void log() {
    if(debug){
      System.out.println("# of buffering events : " + Integer.toString(buffering));
      System.out.println("# of dispatch events : " + Integer.toString(dispatchCalls));
      System.out.println("# of network calls : " + Integer.toString(networkCalls + networkCallsRepeat) + " = " + Integer.toString(networkCalls) + "+" +  Integer.toString(networkCallsRepeat));
      System.out.println("# of biQuery errors : " + Integer.toString(bqErrors + bqErrorsRepeat) + " = " + Integer.toString(bqErrors) + "+" +  Integer.toString(bqErrorsRepeat));
      System.out.println("# of biQuery failures : " + Integer.toString(bQFailures + bQFailuresRepeat) + " = " + Integer.toString(bQFailures) + "+" +  Integer.toString(bQFailuresRepeat));
    }
  }
}

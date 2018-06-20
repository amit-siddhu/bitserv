package com.coverfox.bitserv;


// use event bus instead [LATER]

public class MetricAnalyser {
  public static Integer buffering = 0;
  public static Integer dispatchCalls = 0;

  public static Integer networkCalls = 0;
  public static Integer networkCallsRepeat = 0;
  
  public static Integer networkFailures = 0;
  public static Integer networkFailuresRepeat = 0;
  
  public static Integer bQFailures = 0;
  public static Integer bQFailuresRepeat = 0;
  
  public static void dispatchCall(Integer dispatchSize, String identifier){
    System.out.println("[DISPATCH] (" + identifier +") : "+ Integer.toString(dispatchSize));
    dispatchCalls++;
  }
  // print time taken for the network call
  public static void networkCall(Integer repeatIndex){
    if (repeatIndex == 2) {
      networkCallsRepeat++;
      System.out.println("[NETWORK] 2");
    }
    else networkCall();
  }
  public static void networkCall(){
    System.out.println("[NETWORK]");
    networkCalls++;
  }
  public static void buffering(){
    buffering++;
  }
  public static void networkFailure(){
    System.out.println("[NETWORK FAILURE]");
    networkFailures++;
  }
  public static void networkFailure(Integer repeatIndex){
    if (repeatIndex == 2) {
      networkCallsRepeat++;
      System.out.println("[NETWORK FAILURE] 2");
    }
    else networkFailure();
  }
  public static void BigqueryFailure(){
    System.out.println("[BIGQUERY FAILURE]");
    bQFailures++;
  }
  public static void BigqueryFailure(Integer repeatIndex){
    if (repeatIndex == 2) {
      bQFailuresRepeat++;
      System.out.println("[BIGQUERY FAILURE] 2");
    }
    else networkFailure();
  }
  public static void log() {
    System.out.println("# of buffering events : " + Integer.toString(buffering));
    System.out.println("# of dispatch events : " + Integer.toString(dispatchCalls));
    System.out.println("# of network calls : " + Integer.toString(networkCalls + networkCallsRepeat) + " = " + Integer.toString(networkCalls) + "+" +  Integer.toString(networkCallsRepeat));
    System.out.println("# of network failures : " + Integer.toString(networkFailures + networkFailuresRepeat) + " = " + Integer.toString(networkFailures) + "+" +  Integer.toString(networkFailuresRepeat));
    System.out.println("# of biQuery failures : " + Integer.toString(bQFailures + bQFailuresRepeat) + " = " + Integer.toString(bQFailures) + "+" +  Integer.toString(bQFailuresRepeat));
  }
}

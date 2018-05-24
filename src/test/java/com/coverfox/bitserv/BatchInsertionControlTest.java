package com.coverfox.bitserv;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.json.JSONObject;


@RunWith(JUnit4.class)
public class BatchInsertionControlTest {
  private ByteArrayOutputStream bout;
  private PrintStream out;

  @Before
  public void setUp() {
    bout = new ByteArrayOutputStream();
    out = new PrintStream(bout);
    System.setOut(out);
  }

  @After
  public void tearDown() {
    System.setOut(null);
  }

  @Test
  public void testQuickstart() throws Exception {
    BatchInsertionControl control = BatchInsertionControl.getInstance();
    String got = control.toString();

    // String message = new String('{"target":"table","action":"insert","data":{"schema":{"name":"testTable","dataset":"testDataset","rows":[{"insertId":"123456","json":{"expired":"true","screenshot":"Cg0NDg0","registrationDate":"2017-5-16","createdOn":"2017-05-16 21:52:21.12","id":"123456","response":{"finalPremium":"23769","idv":["855000","885000","915000"]},"insurerQuotes":[{"premium":"23768.55","insurer":"oriental","cost":"0:0:3.123","createdOn":"1494929154"},{"premium":"21768.55","insurer":"kotak","cost":"0:0:1.123","createdOn":"1494929155"}]}},{"insertId":"123457","json":{"expired":"false","screenshot":"Cg0NDg0","registrationDate":"2017-5-15","createdOn":"2017-06-15 21:52:21.12","id":"123457","response":{"finalPremium":"23769","idv":["855000","885000","915000"]},"insurerQuotes":[{"premium":"24768.55","insurer":"oriental","cost":"0:0:3.123","createdOn":"1494929154"}]}}]}}}');
    String message = "{\"target\":\"table\",\"action\":\"insert\",\"data\":{\"schema\":{\"name\":\"testTable\",\"dataset\":\"testDataset\",\"rows\":[{\"insertId\":\"123456\",\"json\":{\"expired\":\"true\",\"screenshot\":\"Cg0NDg0\",\"registrationDate\":\"2017-5-16\",\"createdOn\":\"2017-05-16 21:52:21.12\",\"id\":\"123456\",\"response\":{\"finalPremium\":\"23769\",\"idv\":[\"855000\",\"885000\",\"915000\"]},\"insurerQuotes\":[{\"premium\":\"23768.55\",\"insurer\":\"oriental\",\"cost\":\"0:0:3.123\",\"createdOn\":\"1494929154\"},{\"premium\":\"21768.55\",\"insurer\":\"kotak\",\"cost\":\"0:0:1.123\",\"createdOn\":\"1494929155\"}]}},{\"insertId\":\"123457\",\"json\":{\"expired\":\"false\",\"screenshot\":\"Cg0NDg0\",\"registrationDate\":\"2017-5-15\",\"createdOn\":\"2017-06-15 21:52:21.12\",\"id\":\"123457\",\"response\":{\"finalPremium\":\"23769\",\"idv\":[\"855000\",\"885000\",\"915000\"]},\"insurerQuotes\":[{\"premium\":\"24768.55\",\"insurer\":\"oriental\",\"cost\":\"0:0:3.123\",\"createdOn\":\"1494929154\"}]}}]}}}";
    JSONObject jmessage = new JSONObject(message);
    jmessage = jmessage.getJSONObject("data");
    control.buffer(jmessage);
    System.out.println(control.getBufferedRequests().toString());
  }
}

package com.github.ambry.clustermap;

import java.util.Iterator;
import org.json.JSONException;
import org.json.JSONObject;


public class Port {
  int portNo;
  PortType type;

  public Port(int port, PortType type) {
    this.portNo = port;
    this.type = type;
  }

  public Port(JSONObject jsonObject) throws JSONException{
    Iterator<String> itr = jsonObject.keys();
    while(itr.hasNext()){
      String portType = itr.next();
      portNo = Integer.parseInt(jsonObject.get(portType).toString());
      type = getType(portType);
    }
  }

  private PortType getType(String portType){
    if(portType.equalsIgnoreCase(PortType.PLAINTEXT.toString())) {
      return PortType.PLAINTEXT;
    }
    else if (portType.equalsIgnoreCase(PortType.SSL.toString())) {
      return PortType.SSL;
    }
    else {
      throw new IllegalStateException("Port type " + portType +" unknown");
    }
  }

  public int getPortNo() {
    return this.portNo;
  }

  public PortType getType(){
    return this.type;
  }

  @Override
  public String toString() {
    return "Ports[" + getPortNo() + ":" + getType() + "]";
  }

  public JSONObject toJSONObject()
      throws JSONException {
    JSONObject jsonObject = new JSONObject().put(getType().toString().toLowerCase(), getPortNo());
    return jsonObject;
  }
}

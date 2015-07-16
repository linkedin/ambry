package com.github.ambry.network;

import org.json.JSONException;
import org.json.JSONObject;



public class Port{
  int portNo;
  PortType type;

  public Port(int port, PortType type) {
    this.portNo = port;
    this.type = type;
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

  public PortType getPortType(){
    return this.type;
  }

  @Override
  public String toString() {
    return "Ports[" + getPortNo() + ":" + getPortType() + "]";
  }

  public JSONObject toJSONObject()
      throws JSONException {
    JSONObject jsonObject = new JSONObject().put(getPortType().toString().toLowerCase(), getPortNo());
    return jsonObject;
  }
}

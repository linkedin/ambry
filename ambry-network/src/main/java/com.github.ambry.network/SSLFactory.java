package com.github.ambry.network;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

public class SSLFactory {

  private String protocol;
  private String provider;
  private String kmfAlgorithm;
  private String tmfAlgorithm;
  private SecurityStore keyStore;
  private String keyPassword;
  private SecurityStore trustStore;
  private String[] cipherSuites;
  private String[] enabledProtocols;

  public SSLContext createSSLContext()
      throws GeneralSecurityException, IOException {
    SSLContext sslContext;
    if (provider != null) {
      sslContext = SSLContext.getInstance(protocol, provider);
    } else {
      sslContext = SSLContext.getInstance(protocol);
    }

    KeyManager[] keyManagers = null;
    if (keyStore != null) {
      String kmfAlgorithm = this.kmfAlgorithm != null ? this.kmfAlgorithm : KeyManagerFactory.getDefaultAlgorithm();
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
      KeyStore ks = keyStore.load();
      String keyPassword = this.keyPassword != null ? this.keyPassword : keyStore.password;
      kmf.init(ks, keyPassword.toCharArray());
      keyManagers = kmf.getKeyManagers();
    }

    String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
    KeyStore ts = trustStore == null ? null : trustStore.load();
    tmf.init(ts);

    sslContext.init(keyManagers, tmf.getTrustManagers(), null);
    return sslContext;
  }

  public SSLEngine createSSLEngine(SSLContext sslContext, String peerHost, int peerPort, boolean useClientMode) {
    SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
    if (cipherSuites != null) {
      sslEngine.setEnabledCipherSuites(cipherSuites);
    }
    sslEngine.setUseClientMode(useClientMode);
    if (enabledProtocols != null) {
      sslEngine.setEnabledProtocols(enabledProtocols);
    }
    return sslEngine;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public void setKeyManagerFactoryAlgorithm(String algorithm) {
    this.kmfAlgorithm = algorithm;
  }

  public void setTrustManagerFactoryAlgorithm(String algorithm) {
    this.tmfAlgorithm = algorithm;
  }

  public void setKeyStore(String type, String path, String password, String keyPassword)
      throws Exception {
    if (path == null && password != null) {
      throw new Exception("SSL key store password is not specified.");
    } else if (path != null && password == null) {
      throw new Exception("SSL key store is not specified, but key store password is specified.");
    } else if (path != null && password != null) {
      this.keyStore = new SecurityStore(type, path, password);
      this.keyPassword = keyPassword;
    }
  }

  public void setTrustStore(String type, String path, String password)
      throws Exception {
    if (path == null && password != null) {
      throw new Exception("SSL key store password is not specified.");
    } else if (path != null && password == null) {
      throw new Exception("SSL key store is not specified, but key store password is specified.");
    } else if (path != null && password != null) {
      this.trustStore = new SecurityStore(type, path, password);
    }
  }

  public void setCipherSuites(List<String> cipherSuites) {
    if (cipherSuites != null && cipherSuites.size() > 0) {
      this.cipherSuites = cipherSuites.toArray(new String[cipherSuites.size()]);
    }
  }

  public void setEnabledProtocols(List<String> enabledProtocols) {
    if (enabledProtocols != null && enabledProtocols.size() > 0) {
      this.enabledProtocols = enabledProtocols.toArray(new String[enabledProtocols.size()]);
    }
  }

  private class SecurityStore {
    private final String type;
    private final String path;
    private final String password;

    private SecurityStore(String type, String path, String password) {
      this.type = type == null ? KeyStore.getDefaultType() : type;
      this.path = path;
      this.password = password;
    }

    private KeyStore load()
        throws GeneralSecurityException, IOException {
      FileInputStream in = null;
      try {
        KeyStore ks = KeyStore.getInstance(type);
        in = new FileInputStream(path);
        ks.load(in, password.toCharArray());
        return ks;
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }
  }
}

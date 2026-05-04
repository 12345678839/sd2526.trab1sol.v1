package sd2526.trab.impl.utils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

public class SslContextFactory {

  public static SSLContext getContext(String keystoreFile, String password) throws Exception {
    KeyStore ks = KeyStore.getInstance("JKS");
    try (FileInputStream fis = new FileInputStream("/home/sd/" + keystoreFile)) {
      ks.load(fis, password.toCharArray());
    }

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, password.toCharArray());

    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    return sslContext;
  }
}
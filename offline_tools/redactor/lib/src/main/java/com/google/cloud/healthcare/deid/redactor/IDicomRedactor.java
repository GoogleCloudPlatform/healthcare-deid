package com.google.cloud.healthcare.deid.redactor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface IDicomRedactor {
  public void redact(InputStream inStream, OutputStream outStream)
      throws IOException, IllegalArgumentException;

}

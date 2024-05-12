package org.jeong.etc;

import java.io.File;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class JarMainFest {

    private String version = null;

    public JarMainFest(URL location, String project) {
        try {
            File file = new File(location.toURI());
            if (file.isFile()) {
                try (JarFile jarFile = new JarFile(file)) {
                    Manifest manifest = jarFile.getManifest();
                    Attributes attributes = manifest.getMainAttributes();
                    this.version = attributes.getValue(project);
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to load jar file or read manifest: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public String getVersion() {
        return this.version;
    }
}

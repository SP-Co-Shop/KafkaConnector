package org.jeong.etc;

import java.io.File;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class JarMainFest {

    private String project;

    private String msg = "Pass";
    private String version = null;

    public JarMainFest(URL location, String project) {

        this.project = project;

        try{

            File file = new File(location.toURI());

            if (file.isFile()){
                Manifest manifest = new JarFile(file).getManifest();
                Attributes attributes = manifest.getMainAttributes();

                this.version = attributes.getValue(this.project);
            }


        } catch (Exception e) {
            msg = e.getMessage();
        }
    }

    public String getVersion() {
        return this.version;
    }
}

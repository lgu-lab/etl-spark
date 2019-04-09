package org.demo.framework;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class FileLoader {
	
    public static String loadFile(String filePath) {
    	return loadFile(new File(filePath));
    }
    
    public static String loadFile(File file) {
    	if ( file.exists() ) {
        	return loadFile(file.toURI());
    	}
    	else {
    		throw new RuntimeException("File '" + file.getAbsolutePath() + "' doesn't exist.");
    	}
    }
    
    public static String loadFile(URI fileURI) {

        StringBuilder sb = new StringBuilder();

        Path filePath = Paths.get(fileURI);
        try ( BufferedReader br = Files.newBufferedReader(filePath) ) {
            // read line by line
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } catch (IOException e) {
    		throw new RuntimeException("Cannot load file '" + fileURI + "' IOException.");
        }

        return sb.toString();
    }

}

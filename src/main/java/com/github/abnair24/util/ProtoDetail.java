package com.github.abnair24.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ProtoDetail {

    private static final Logger logger = LoggerFactory.getLogger(ProtoDetail.class);

    private final String protoPath;
    private final String packageName;
    private final String methodName;
    private final List<String> protoFiles;


    /**
     * @param protoPath
     * @param fullMethodName
     */
    public ProtoDetail(String protoPath, String fullMethodName) {

        this.protoPath = protoPath;
        this.packageName = findPackageName(fullMethodName);
        this.methodName = findMessageName(fullMethodName, packageName.length());
        this.protoFiles = getAllProtoFiles(protoPath);
    }

    private List<String> getAllProtoFiles(String protoPath) {
        List<String> protoFilesPaths = new ArrayList<>();

        Path path = Paths.get(protoPath);

        try(DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path,"*.proto")) {
            directoryStream.forEach(p -> protoFilesPaths.add(p.toString()));
        } catch(IOException ex) {
            logger.error("Proto path error",ex);
        }
        return protoFilesPaths;
    }

    private String findPackageName(String fullMethodName) {
        return fullMethodName.substring(0, fullMethodName.lastIndexOf('.'));
    }

    private String findMessageName(String fullMethodName, int length) {
        return fullMethodName.substring(length + 1);
    }

    public String getProtoPath() {
        return protoPath;
    }

    public String getPackageName() {
        return packageName;
    }

    public String getMethodName() {
        return methodName;
    }

    public List<String> getProtoFiles() {
        return protoFiles;
    }
}

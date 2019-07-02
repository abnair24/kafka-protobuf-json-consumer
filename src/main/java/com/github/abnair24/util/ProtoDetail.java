package com.github.abnair24.util;

import io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ProtoDetail {

    private static final Logger logger = LoggerFactory.getLogger(ProtoDetail.class);

    private final String protoPath;
    private final String serviceName;
    private final String packageName;
    private final String methodName;
    private final List<String> protoFilesPath;

    public ProtoDetail(String protoPath, String fullMethodName) {

        String fullService = MethodDescriptor.extractFullServiceName(fullMethodName);

        if(fullService == null) {
            throw new IllegalArgumentException("Failed extracting service name"+fullMethodName);
        }

        this.protoPath = protoPath;
        this.serviceName = getServiceName(fullService, fullMethodName.lastIndexOf('.'));
        this.packageName = getPackageName(fullMethodName);
        this.methodName = getMethodName(fullMethodName, fullService.length());
        this.protoFilesPath = getAllProtoFiles(protoPath);
    }

    private List<String> getAllProtoFiles(String protoPath) {
        List<String> protoFilesPaths = new ArrayList<>();

        Path path = Paths.get(protoPath);

        try(DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path,"*.proto")) {
            directoryStream.forEach(p -> protoFilesPaths.add(p.toString()));
        } catch(IOException ex) {
            logger.error("Proto path error");
        }
        return protoFilesPaths;
    }

    private String getServiceName(String fullService, int i) {
        return getMethodName(fullService, i);
    }

    private String getPackageName(String fullMethodName) { return fullMethodName.substring(0, fullMethodName.lastIndexOf('.'));}

    private String getMethodName(String fullMethodName, int length) {
        return fullMethodName.substring(length + 1);
    }

    public String getProtoPath() {
        return protoPath;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getPackageName() {
        return packageName;
    }

    public String getMethodName() {
        return methodName;
    }

    public String getMethodFullName() { return this.getPackageName()+"."+ this.getServiceName()+"/"+this.getMethodName(); }

    public List<String> getProtoFilesPath() {
        return protoFilesPath;
    }
}

package com.github.abnair24.util;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
@Getter
public class ProtoDetail {

    private final String protoPath;
    private final String packageName;
    private final String methodName;
    private final List<String> protoFiles;
    private String descriptorBinaryPath;

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
            log.error("Proto path error : {}",path.toString());
        }
        return protoFilesPaths;
    }

    private String findPackageName(String fullMethodName) {
        return fullMethodName.substring(0, fullMethodName.lastIndexOf('.'));
    }

    private String findMessageName(String fullMethodName, int length) {
        return fullMethodName.substring(length + 1);
    }

    public void setDescriptorBinaryPath(String descriptorBinaryPath) {
        this.descriptorBinaryPath = descriptorBinaryPath;
    }
}

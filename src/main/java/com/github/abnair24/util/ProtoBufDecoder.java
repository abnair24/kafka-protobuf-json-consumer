package com.github.abnair24.util;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;

import java.util.ArrayList;
import java.util.List;

public class ProtoBufDecoder {

    private static final Logger logger = LoggerFactory.getLogger(ProtoBufDecoder.class);

    public static Descriptors.Descriptor getDescriptor(ProtoDetail protoDetail,
                                                              String protoDescriptorFilePath) throws Exception {

        DescriptorProtos.FileDescriptorSet fdSet = DescriptorProtos
                .FileDescriptorSet.parseFrom(new FileInputStream(protoDescriptorFilePath));

        List<Descriptors.FileDescriptor> fdList = new ArrayList<>();

        for(DescriptorProtos.FileDescriptorProto fileDescriptorProto: fdSet.getFileList()) {
            fdList.add(ProtoCache.getFileDescriptor(fileDescriptorProto));
        }
        return resolveMethodDescriptor(protoDetail,fdList);
    }


    public static Descriptors.FileDescriptor getAllFileDescriptors(DescriptorProtos.FileDescriptorProto fileDescriptorProto) throws Exception {

        Descriptors.FileDescriptor fileDescriptor =null;
        List<String>dependencies = fileDescriptorProto.getDependencyList();

        List<Descriptors.FileDescriptor> fdlist = new ArrayList<>();
        for(String dep : dependencies) {
            Descriptors.FileDescriptor fd = null;
            for(DescriptorProtos.FileDescriptorProto fdp : ProtoCache.getAllFileDescriptorFromCache()) {
                if (dep.equals(fdp.getName())) {
                    fd = ProtoCache.getFileDescriptor(fdp);
                }
            }
            if(fd!=null) {
                fdlist.add(fd);
            }
        }
        if(fdlist.size()== dependencies.size()) {
            Descriptors.FileDescriptor[] fds = new Descriptors.FileDescriptor[fdlist.size()];
            fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto,fdlist.toArray(fds));
        }
        return fileDescriptor;
    }


    private static Descriptors.Descriptor resolveMethodDescriptor(ProtoDetail protoDetail,
                                                                        List<Descriptors.FileDescriptor>fileDescriptorList) {
        String methodName = protoDetail.getMethodName();
        String packageName = protoDetail.getPackageName();

        Descriptors.Descriptor descriptor = null;

        for (Descriptors.FileDescriptor fileDescriptor : fileDescriptorList) {
            if (!isPackageSame(fileDescriptor, packageName)) {
                continue;
            }
            descriptor = getMethodDescriptor(fileDescriptor, methodName);
            break;
        }

        return descriptor;
    }

    private static Descriptors.Descriptor getMethodDescriptor(Descriptors.FileDescriptor fileDescriptor, String methodName) {
        Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(methodName);
        if(descriptor == null) {
            throw new IllegalArgumentException("Wrong method name parsed :"+ methodName);
        }
        return descriptor;
    }

    private static boolean isPackageSame(Descriptors.FileDescriptor fileDescriptor,String packageName) {
        boolean status;
        if(packageName != null && packageName.equals(fileDescriptor.getPackage())) {
            status = true;
        } else {
            status  = false;
        }
        return status;
    }
}

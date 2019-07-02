package com.github.abnair24.util;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ProtoBufDecoder {

    private static Descriptors.MethodDescriptor getDescriptor(ProtoDetail protoDetail,
                                                              String protoDescriptorFilePath) throws Exception {

        DescriptorProtos.FileDescriptorSet fdSet = DescriptorProtos
                .FileDescriptorSet.parseFrom(new FileInputStream(protoDescriptorFilePath));

        List<Descriptors.FileDescriptor> fdList = new ArrayList<>();

        for(DescriptorProtos.FileDescriptorProto fileDescriptorProto: fdSet.getFileList()) {
            fdList.add(ProtoCache.getFileDescriptor(fileDescriptorProto));
        }
        return getMethodDescriptor(protoDetail,fdList);
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

    public static Descriptors.MethodDescriptor getMethodDescriptor(ProtoDetail protoDetail, Path descFile) throws Exception {
        return ProtoBufDecoder.getDescriptor(protoDetail,descFile.toAbsolutePath().toString());
    }

    private static Descriptors.MethodDescriptor getMethodDescriptor(ProtoDetail protoDetail,
                                                                    List<Descriptors.FileDescriptor>fileDescriptorList) {
        String serviceName = protoDetail.getServiceName();
        String methodName = protoDetail.getMethodName();
        String packageName = protoDetail.getPackageName();

        Descriptors.ServiceDescriptor serviceDescriptor=null;

        for(Descriptors.FileDescriptor fileDescriptor : fileDescriptorList) {
            if (!fileDescriptor.getPackage().equals(packageName)) {
                continue;
            } else {
                serviceDescriptor = fileDescriptor.findServiceByName(serviceName);

                if (serviceDescriptor == null) {
                    throw new IllegalArgumentException("Service Not found :" + serviceName);
                }
                break;
            }
        }
        return serviceDescriptor.findMethodByName(methodName);
    }
}

package com.github.abnair24.jsonFormatter;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

public class JsonFormater {

    public static String toJson(Message message) throws Exception {
        JsonFormat.Printer printer = JsonFormat.printer();
        return printer.print(message);
    }

    public static JsonObject toJsonObject(Message message) throws Exception {
        JsonFormat.Printer printer = JsonFormat.printer();
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(printer.print(message)).getAsJsonObject();

        return jsonObject;
    }
}

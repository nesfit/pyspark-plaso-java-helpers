package tarzan.helpers.py4j;

import com.google.gson.*;

import java.util.*;

/**
 * Helpers to convert Python Pickle/Pyrolite objects into Java-native objects by a conversion to/from JSON.
 */
public class PythonObjectsByGson {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
    private static final ConversionException MAX_DEPTH_EXCEPTION =
            new ConversionException("A maximum depth of the dive into the JSON tree has been reached.");

    /**
     * Conversion exception.
     */
    public static class ConversionException extends Exception {
        private JsonElement jsonElement;

        ConversionException(String message) {
            super(message);
            this.jsonElement = null;
        }

        ConversionException(String message, JsonElement jsonElement) {
            super(message);
            this.jsonElement = jsonElement;
        }

        /**
         * Get JSON element that caused the exception.
         *
         * @return the JSON element
         */
        public JsonElement getJsonElement() {
            return jsonElement;
        }
    }

    private static Object jsonPrimitiveToJavaObject(JsonPrimitive jsonPrimitive) throws ConversionException {
        if (jsonPrimitive.isBoolean()) {
            return jsonPrimitive.getAsBoolean();
        } else if (jsonPrimitive.isNumber()) {
            return jsonPrimitive.getAsNumber();
        } else if (jsonPrimitive.isString()) {
            return jsonPrimitive.getAsString();
        }
        throw new ConversionException("JSONPrimitive is none of Boolean, Number, or String.", jsonPrimitive);
    }

    private static Object jsonArrayToJavaObject(JsonArray jsonArray, int depth, boolean flattenSingleElementContainers) throws ConversionException {
        if (flattenSingleElementContainers && (jsonArray.size() <= 1)) {
            // flattening does not decrease depth of the dive
            return jsonArray.size() == 0 ? null : jsonTreeToJavaObject(jsonArray.get(0), depth, flattenSingleElementContainers);
        } else {
            final List<Object> objectList = new ArrayList<>();
            for (JsonElement jsonElement : jsonArray) {
                objectList.add(jsonTreeToJavaObject(jsonElement, depth - 1, flattenSingleElementContainers));
            }
            return objectList;
        }
    }

    private static Map<String, Object> jsonObjectToAttrValMap(JsonObject jsonObject, int depth, boolean flattenSingleElementContainers) throws ConversionException {
        final Map<String, Object> attrValMap = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            attrValMap.put(entry.getKey(), jsonTreeToJavaObject(entry.getValue(), depth - 1, flattenSingleElementContainers));
        }
        return Collections.unmodifiableMap(attrValMap);
    }

    /**
     * Convert a JSON element tree into a Java-native object.
     *
     * @param jsonTree                       the JSON element tree
     * @param depth                          the depth of a dive into the JSON tree
     * @param flattenSingleElementContainers if translate a single-item containers (such as arrays) into their single members
     * @return the corresponding Java-native object
     * @throws ConversionException if there is an error in the conversion
     */
    public static Object jsonTreeToJavaObject(JsonElement jsonTree, int depth, boolean flattenSingleElementContainers) throws ConversionException {
        if (depth == 0) {
            return MAX_DEPTH_EXCEPTION;
        } else if (jsonTree.isJsonNull()) {
            return null;
        } else if (jsonTree.isJsonPrimitive()) {
            return jsonPrimitiveToJavaObject(jsonTree.getAsJsonPrimitive());
        } else if (jsonTree.isJsonArray()) {
            return jsonArrayToJavaObject(jsonTree.getAsJsonArray(), depth, flattenSingleElementContainers);
        } else if (jsonTree.isJsonObject()) {
            return jsonObjectToAttrValMap(jsonTree.getAsJsonObject(), depth, flattenSingleElementContainers);
        }
        throw new ConversionException("JSONElement is none of JSON-Null, -Primitive, -Array, or -Object.", jsonTree);
    }

    /**
     * Convert a Python Pickle/Pyrolite objects into a JSON element tree.
     *
     * @param object the Python Pickle/Pyrolite object
     * @return the corresponding JSON element tree
     */
    public static JsonElement objectToJsonTree(Object object) {
        return GSON.toJsonTree(object);
    }

    /**
     * Convert a Python Pickle/Pyrolite objects into a JSON document.
     *
     * @param object the Python Pickle/Pyrolite object
     * @return the corresponding JSON document as a string
     */
    public static String objectToJsonString(Object object) {
        return GSON.toJson(object);
    }

    /**
     * Convert a Python Pickle/Pyrolite objects into a Java-native object.
     *
     * @param object the Python Pickle/Pyrolite object
     * @return the corresponding JSON document as a string
     * @throws ConversionException if there is an error in the conversion
     */
    public static Object objectToJavaObject(Object object, int depth, boolean flattenSingleElementContainers) throws ConversionException {
        return jsonTreeToJavaObject(GSON.toJsonTree(object), depth, flattenSingleElementContainers);
    }
}

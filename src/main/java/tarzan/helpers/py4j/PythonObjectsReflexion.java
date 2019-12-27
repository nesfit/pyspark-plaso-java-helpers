package tarzan.helpers.py4j;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helpers to convert Python Pickle/Pyrolite objects into Java-native objects by a direct conversion.
 * This is not able to convert String components of RDD tuples (i.e., directly in the RDD, not in object-items).
 */
public class PythonObjectsReflexion {
    /**
     * Returns a string representation of a given object.
     *
     * @param object an object
     * @return the string representation of the object
     */
    public static String objectToString(Object object) {
        //return String.format("%s: %s\n", object.getClass().getCanonicalName(), object.toString()); //*
        //return new GsonBuilder().setPrettyPrinting().serializeNulls().create().toJson(object); //*
        StringBuilder stringBuilder = new StringBuilder();
        final Object[] objects = pythonArrayToObjectArray(object);
        if (objects != null) {
            stringBuilder.append("[\n");
            for (Object objectsItem : objects) {
                stringBuilder.append(objectToString(objectsItem));
            }
            stringBuilder.append("]\n");
        } else {
            stringBuilder.append("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : objectToAttrValMap(object).entrySet()) {
                if (first) {
                    first = false;
                } else {
                    stringBuilder.append(", ");
                }
                final Object value = entry.getValue();
                stringBuilder.append(entry.getKey());
                stringBuilder.append("=");
                if (value instanceof String) {
                    stringBuilder.append('"');
                    stringBuilder.append(value);
                    stringBuilder.append('"');
                } else {
                    stringBuilder.append(value);
                }
            }
            stringBuilder.append("}\n");
        }
        return stringBuilder.toString();
        //*/
    }

    /**
     * Access a given Java object which is a Java array as an Java object array of objects.
     *
     * @param object an object to access as the objects array
     * @return the objects array is possible
     */
    public static Object[] objectToObjectArray(Object object) {
        final Class componentsClass = object.getClass().getComponentType();
        if (componentsClass == null) {
            return null;
        } else if (componentsClass.isPrimitive()) {
            // an array of primitives must be copied into a new array of objects
            final int length = Array.getLength(object);
            final Object[] objects = new Object[length];
            for (int i = 0; i < length; i++) {
                objects[i] = Array.get(object, i);
            }
            return objects;
        } else {
            // an array of objects can be returned as is
            return (Object[]) object;
        }
    }

    /**
     * Access a given Java object which is a Python array as an Java object array of objects.
     *
     * @param object a Python array to access as the objects array
     * @return the objects array if possible
     */
    public static Object[] pythonArrayToObjectArray(Object object) {
        final Class objectsClass = object.getClass();
        // the object is a common Java array
        if (objectsClass.isArray()) {
            return objectToObjectArray(object);
        }
        // the object can be a Python array
        final String fieldSizeName = "size";
        final String fieldElementDataName = "elementData";
        try {
            final Field fieldElementData = objectsClass.getDeclaredField(fieldElementDataName);
            fieldElementData.setAccessible(true);
            final Field fieldSize = objectsClass.getDeclaredField(fieldSizeName);
            fieldSize.setAccessible(true);
            final int size = fieldSize.getInt(object);
            final Object elementData = fieldElementData.get(object);
            // not an array
            if (!elementData.getClass().isArray()) {
                return null;
            }
            // check the array's length which must be equal to the declared size
            final int length = Array.getLength(elementData);
            if (size != length) {
                return null;
            }
            // get the object array
            return objectToObjectArray(elementData);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return null;
        }
    }

    /**
     * List attributes and corresponding values of a given object.
     *
     * @param object an object
     * @return the map of the object's attributes and their values
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> objectToAttrValMap(Object object) {
        final Map<String, Object> attrValMap;
        if (object instanceof Map<?, ?>) {
            // Map can be passed directly, e.g., net.razorvine.pickle.objects.ClassDict is Map<String, Object>
            attrValMap = (Map<String, Object>) object;
        } else {
            // other objects need to be accessed by reflexion
            attrValMap = new HashMap<>();
            final Field[] fields = object.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    field.setAccessible(true);
                    final String key = field.getName();
                    try {
                        final Object value = field.get(object);
                        attrValMap.put(key, value);
                    } catch (IllegalAccessException e) {
                        attrValMap.put(key, e);
                    }
                }
            }
        }
        return Collections.unmodifiableMap(attrValMap);
    }

    public static Map<String, String> stringObjectMapToStringStringMap(Map<String, Object> stringObjectMap) {
        return stringObjectMap.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, value -> value.getValue().toString()));
    }
}

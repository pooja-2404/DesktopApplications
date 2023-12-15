public class ObjectArrayExample {
    public static void main(String[] args) {
        // Create an Object[] array
        Object[] objectArray = new Object[5];

        // Store various objects in the array
        objectArray[0] = "Hello, World!"; // String
        objectArray[1] = 42; // Integer (autoboxing)
        objectArray[2] = 3.14159; // Double (autoboxing)
        objectArray[3] = new StringBuilder("Java is fun!"); // StringBuilder
        objectArray[4] = new Object(); // Generic Object

        // Access and use objects from the array
        for (Object obj : objectArray) {
            System.out.println(obj);
        }
    }
}

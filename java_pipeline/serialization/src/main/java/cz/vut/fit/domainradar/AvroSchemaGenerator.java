package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.ip.GeoIPData;
import cz.vut.fit.domainradar.models.ip.NERDData;
import cz.vut.fit.domainradar.models.ip.RTTData;
import cz.vut.fit.domainradar.models.results.CommonIPResult;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class AvroSchemaGenerator {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Exactly one argument, the output directory, must be passed.");
            System.exit(1);
        }

        var outputDir = args[0];
        ensureOutputDir(outputDir);

        final ObjectMapper jsonMapper = Common.makeMapper().build();

        var types = new ArrayList<JavaType>();
        types.add(jsonMapper.constructType(new TypeReference<CommonIPResult<GeoIPData>>() {
        }));
        types.add(jsonMapper.constructType(new TypeReference<CommonIPResult<NERDData>>() {
        }));
        types.add(jsonMapper.constructType(new TypeReference<CommonIPResult<RTTData>>() {
        }));
        types.add(jsonMapper.constructType(new TypeReference<CommonIPResult<byte[]>>() {
        }));

        try {
            Class[] classes = getClasses("cz.vut.fit.domainradar.models");
            for (Class clazz : classes) {
                if (clazz.getName().contains("ResultCodes"))
                    continue;
                if (clazz.isInterface())
                    continue;

                var type = jsonMapper.constructType(clazz);
                types.add(type);
            }

            for (var type : types) {
                var gen = new com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator();
                gen.enableLogicalTypes();

                jsonMapper.acceptJsonFormatVisitor(type, gen);

                var schema = gen.getGeneratedSchema();
                var label = Common.getTypeLabel(type);
                var schemaFile = new File(outputDir, label + ".avsc");

                try (var writer = new java.io.FileWriter(schemaFile)) {
                    writer.write(schema.getAvroSchema().toString(true));
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void ensureOutputDir(String outputDir) {
        var dir = new File(outputDir);
        if (dir.isFile()) {
            System.err.println("Output directory is a file.");
            System.exit(1);
        }
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.err.println("Failed to create output directory.");
                System.exit(1);
            }
        }
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @param packageName The base package
     * @return The classes
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private static Class[] getClasses(String packageName)
            throws ClassNotFoundException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources = classLoader.getResources(path);
        List<File> dirs = new ArrayList<File>();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }
        ArrayList<Class> classes = new ArrayList<Class>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes.toArray(new Class[classes.size()]);
    }

    /**
     * Recursive method used to find all classes in a given directory and subdirs.
     *
     * @param directory   The base directory
     * @param packageName The package name for classes found inside the base directory
     * @return The classes
     * @throws ClassNotFoundException
     */
    private static List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
        List<Class> classes = new ArrayList<Class>();
        if (!directory.exists()) {
            return classes;
        }
        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file, packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
            }
        }
        return classes;
    }
}

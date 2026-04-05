error id: jar:file://<HOME>/.m2/repository/org/apache/tomcat/embed/tomcat-embed-core/11.0.20/tomcat-embed-core-11.0.20-sources.jar!/org/apache/catalina/authenticator/jaspic/PersistentProviderRegistrations.java
file://<WORKSPACE>/jar:file:<HOME>/.m2/repository/org/apache/tomcat/embed/tomcat-embed-core/11.0.20/tomcat-embed-core-11.0.20-sources.jar!/org/apache/catalina/authenticator/jaspic/PersistentProviderRegistrations.java
### java.lang.Exception: Unexpected symbol '\' at word pos: '5959' Line: '                writer.write(">\\n");'

Java indexer failed with and exception.
```Java
/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.catalina.authenticator.jaspic;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.digester.Digester;
import org.apache.tomcat.util.res.StringManager;
import org.xml.sax.SAXException;

/**
 * Utility class for the loading and saving of JASPIC persistent provider registrations.
 */
public final class PersistentProviderRegistrations {

    private static final Log log = LogFactory.getLog(PersistentProviderRegistrations.class);
    private static final StringManager sm = StringManager.getManager(PersistentProviderRegistrations.class);


    private PersistentProviderRegistrations() {
        // Utility class. Hide default constructor
    }


    static Providers loadProviders(File configFile) {
        try (InputStream is = new FileInputStream(configFile)) {
            // Construct a digester to read the XML input file
            Digester digester = new Digester();

            try {
                digester.setFeature("http://apache.org/xml/features/allow-java-encodings", true);
            } catch (SAXException se) {
                log.warn(sm.getString("persistentProviderRegistrations.xmlFeatureEncoding"), se);
            }

            digester.setValidating(true);
            digester.setNamespaceAware(true);

            // Create an object to hold the parse results and put it on the top
            // of the digester's stack
            Providers result = new Providers();
            digester.push(result);

            // Configure the digester
            digester.addObjectCreate("jaspic-providers/provider", Provider.class.getName());
            digester.addSetProperties("jaspic-providers/provider");
            digester.addSetNext("jaspic-providers/provider", "addProvider", Provider.class.getName());

            digester.addObjectCreate("jaspic-providers/provider/property", Property.class.getName());
            digester.addSetProperties("jaspic-providers/provider/property");
            digester.addSetNext("jaspic-providers/provider/property", "addProperty", Property.class.getName());

            // Parse the input
            digester.parse(is);

            return result;
        } catch (IOException | ParserConfigurationException | SAXException e) {
            throw new SecurityException(e);
        }
    }


    static void writeProviders(Providers providers, File configFile) {
        File configFileOld = new File(configFile.getAbsolutePath() + ".old");
        File configFileNew = new File(configFile.getAbsolutePath() + ".new");
        File configParent = configFileNew.getParentFile();

        // Remove left over temporary files if present
        if (configFileOld.exists()) {
            if (!configFileOld.delete()) {
                throw new SecurityException(sm.getString("persistentProviderRegistrations.existsDeleteFail",
                        configFileOld.getAbsolutePath()));
            }
        }
        if (configFileNew.exists()) {
            if (!configFileNew.delete()) {
                throw new SecurityException(sm.getString("persistentProviderRegistrations.existsDeleteFail",
                        configFileNew.getAbsolutePath()));
            }
        }
        if (!configParent.exists()) {
            if (!configParent.mkdirs()) {
                throw new SecurityException(
                        sm.getString("persistentProviderRegistrations.mkdirsFail", configParent.getAbsolutePath()));
            }
        }

        // Write out the providers to the temporary new file
        try (OutputStream fos = new FileOutputStream(configFileNew);
                Writer writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
            writer.write("""
                <?xml version='1.0' encoding='utf-8'?>
                <jaspic-providers
                    xmlns="http://tomcat.apache.org/xml"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="http://tomcat.apache.org/xml jaspic-providers.xsd"
                    version="1.0">
                """);
            for (Provider provider : providers.providers) {
                writer.write("  <provider");
                writeOptional("className", provider.getClassName(), writer);
                writeOptional("layer", provider.getLayer(), writer);
                writeOptional("appContext", provider.getAppContext(), writer);
                writeOptional("description", provider.getDescription(), writer);
                writer.write(">\n");
                for (Entry<String,String> entry : provider.getProperties().entrySet()) {
                    writer.write("    <property name=\"");
                    writer.write(entry.getKey());
                    writer.write("\" value=\"");
                    writer.write(entry.getValue());
                    writer.write("\"/>\n");
                }
                writer.write("  </provider>\n");
            }
            writer.write("</jaspic-providers>\n");
        } catch (IOException ioe) {
            if (!configFileNew.delete()) {
                Log log = LogFactory.getLog(PersistentProviderRegistrations.class);
                log.warn(sm.getString("persistentProviderRegistrations.deleteFail", configFileNew.getAbsolutePath()));
            }
            throw new SecurityException(ioe);
        }

        // Move the current file out of the way
        if (configFile.isFile()) {
            if (!configFile.renameTo(configFileOld)) {
                throw new SecurityException(sm.getString("persistentProviderRegistrations.moveFail",
                        configFile.getAbsolutePath(), configFileOld.getAbsolutePath()));
            }
        }

        // Move the new file into place
        if (!configFileNew.renameTo(configFile)) {
            throw new SecurityException(sm.getString("persistentProviderRegistrations.moveFail",
                    configFileNew.getAbsolutePath(), configFile.getAbsolutePath()));
        }

        // Remove the old file
        if (configFileOld.exists() && !configFileOld.delete()) {
            Log log = LogFactory.getLog(PersistentProviderRegistrations.class);
            log.warn(sm.getString("persistentProviderRegistrations.deleteFail", configFileOld.getAbsolutePath()));
        }
    }


    private static void writeOptional(String name, String value, Writer writer) throws IOException {
        if (value != null) {
            writer.write(" " + name + "=\"");
            writer.write(value);
            writer.write("\"");
        }
    }


    public static class Providers {
        private final List<Provider> providers = new ArrayList<>();

        public void addProvider(Provider provider) {
            providers.add(provider);
        }

        public List<Provider> getProviders() {
            return providers;
        }
    }


    public static class Provider {
        private String className;
        private String layer;
        private String appContext;
        private String description;
        private final Map<String,String> properties = new HashMap<>();


        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }


        public String getLayer() {
            return layer;
        }

        public void setLayer(String layer) {
            this.layer = layer;
        }


        public String getAppContext() {
            return appContext;
        }

        public void setAppContext(String appContext) {
            this.appContext = appContext;
        }


        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }


        public void addProperty(Property property) {
            properties.put(property.getName(), property.getValue());
        }

        /**
         * Used by IntrospectionUtils via reflection.
         *
         * @param name  - the name of the property to set on this object
         * @param value - the value to set
         *
         * @see #addProperty(String, String)
         */
        public void setProperty(String name, String value) {
            addProperty(name, value);
        }

        void addProperty(String name, String value) {
            properties.put(name, value);
        }

        public Map<String,String> getProperties() {
            return properties;
        }
    }


    public static class Property {
        private String name;
        private String value;


        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }


        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}

```


#### Error stacktrace:

```
scala.meta.internal.mtags.JavaToplevelMtags.unexpectedCharacter(JavaToplevelMtags.scala:354)
	scala.meta.internal.mtags.JavaToplevelMtags.kwOrIdent$1(JavaToplevelMtags.scala:194)
	scala.meta.internal.mtags.JavaToplevelMtags.parseToken$1(JavaToplevelMtags.scala:258)
	scala.meta.internal.mtags.JavaToplevelMtags.fetchToken(JavaToplevelMtags.scala:264)
	scala.meta.internal.mtags.JavaToplevelMtags.loop(JavaToplevelMtags.scala:75)
	scala.meta.internal.mtags.JavaToplevelMtags.indexRoot(JavaToplevelMtags.scala:43)
	scala.meta.internal.mtags.MtagsIndexer.index(MtagsIndexer.scala:22)
	scala.meta.internal.mtags.MtagsIndexer.index$(MtagsIndexer.scala:21)
	scala.meta.internal.mtags.JavaToplevelMtags.index(JavaToplevelMtags.scala:18)
	scala.meta.internal.mtags.Mtags.extendedIndexing(Mtags.scala:78)
	scala.meta.internal.mtags.SymbolIndexBucket.indexSource(SymbolIndexBucket.scala:133)
	scala.meta.internal.mtags.SymbolIndexBucket.addSourceFile(SymbolIndexBucket.scala:109)
	scala.meta.internal.mtags.SymbolIndexBucket.$anonfun$addSourceJar$2(SymbolIndexBucket.scala:75)
	scala.collection.immutable.List.flatMap(List.scala:283)
	scala.meta.internal.mtags.SymbolIndexBucket.$anonfun$addSourceJar$1(SymbolIndexBucket.scala:71)
	scala.meta.internal.io.PlatformFileIO$.withJarFileSystem(PlatformFileIO.scala:75)
	scala.meta.internal.io.FileIO$.withJarFileSystem(FileIO.scala:33)
	scala.meta.internal.mtags.SymbolIndexBucket.addSourceJar(SymbolIndexBucket.scala:69)
	scala.meta.internal.mtags.OnDemandSymbolIndex.$anonfun$addSourceJar$2(OnDemandSymbolIndex.scala:86)
	scala.meta.internal.mtags.OnDemandSymbolIndex.tryRun(OnDemandSymbolIndex.scala:132)
	scala.meta.internal.mtags.OnDemandSymbolIndex.addSourceJar(OnDemandSymbolIndex.scala:85)
	scala.meta.internal.metals.Indexer.indexJar(Indexer.scala:662)
	scala.meta.internal.metals.Indexer.addSourceJarSymbols(Indexer.scala:647)
	scala.meta.internal.metals.Indexer.processDependencyPath(Indexer.scala:394)
	scala.meta.internal.metals.Indexer.$anonfun$indexDependencyModules$9(Indexer.scala:454)
	scala.meta.internal.metals.Indexer.$anonfun$indexDependencyModules$9$adapted(Indexer.scala:426)
	scala.collection.IterableOnceOps.foreach(IterableOnce.scala:630)
	scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:628)
	scala.collection.AbstractIterable.foreach(Iterable.scala:936)
	scala.collection.IterableOps$WithFilter.foreach(Iterable.scala:906)
	scala.meta.internal.metals.Indexer.$anonfun$indexDependencyModules$3(Indexer.scala:426)
	scala.meta.internal.metals.Indexer.$anonfun$indexDependencyModules$3$adapted(Indexer.scala:424)
	scala.collection.IterableOnceOps.foreach(IterableOnce.scala:630)
	scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:628)
	scala.collection.AbstractIterable.foreach(Iterable.scala:936)
	scala.collection.IterableOps$WithFilter.foreach(Iterable.scala:906)
	scala.meta.internal.metals.Indexer.$anonfun$indexDependencyModules$1(Indexer.scala:424)
	scala.meta.internal.metals.Indexer.$anonfun$indexDependencyModules$1$adapted(Indexer.scala:423)
	scala.collection.IterableOnceOps.foreach(IterableOnce.scala:630)
	scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:628)
	scala.collection.AbstractIterable.foreach(Iterable.scala:936)
	scala.meta.internal.metals.Indexer.indexDependencyModules(Indexer.scala:423)
	scala.meta.internal.metals.Indexer.$anonfun$indexWorkspace$20(Indexer.scala:199)
	scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	scala.meta.internal.metals.TimerProvider.timedThunk(TimerProvider.scala:25)
	scala.meta.internal.metals.Indexer.$anonfun$indexWorkspace$19(Indexer.scala:192)
	scala.meta.internal.metals.Indexer.$anonfun$indexWorkspace$19$adapted(Indexer.scala:188)
	scala.collection.immutable.List.foreach(List.scala:323)
	scala.meta.internal.metals.Indexer.indexWorkspace(Indexer.scala:188)
	scala.meta.internal.metals.Indexer.$anonfun$profiledIndexWorkspace$2(Indexer.scala:58)
	scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	scala.meta.internal.metals.TimerProvider.timedThunk(TimerProvider.scala:25)
	scala.meta.internal.metals.Indexer.$anonfun$profiledIndexWorkspace$1(Indexer.scala:58)
	scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	scala.concurrent.Future$.$anonfun$apply$1(Future.scala:691)
	scala.concurrent.impl.Promise$Transformation.run(Promise.scala:500)
	java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	java.base/java.lang.Thread.run(Thread.java:840)
```
#### Short summary: 

Java indexer failed with and exception.
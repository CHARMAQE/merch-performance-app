error id: jar:file://<HOME>/.m2/repository/org/hibernate/orm/hibernate-core/7.2.7.Final/hibernate-core-7.2.7.Final-sources.jar!/org/hibernate/tool/schema/extract/internal/InformationExtractorPostgreSQLImpl.java
file://<WORKSPACE>/jar:file:<HOME>/.m2/repository/org/hibernate/orm/hibernate-core/7.2.7.Final/hibernate-core-7.2.7.Final-sources.jar!/org/hibernate/tool/schema/extract/internal/InformationExtractorPostgreSQLImpl.java
### java.lang.RuntimeException: Broken file, quote doesn't end.

Java indexer failed with and exception.
```Java
/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.tool.schema.extract.internal;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;
import org.hibernate.tool.schema.extract.spi.NameSpaceIndexesInformation;

import java.sql.SQLException;

/**
 * @since 7.2
 */
public class InformationExtractorPostgreSQLImpl extends InformationExtractorJdbcDatabaseMetaDataImpl {

	public InformationExtractorPostgreSQLImpl(ExtractionContext extractionContext) {
		super( extractionContext );
	}

	@Override
	public boolean supportsBulkPrimaryKeyRetrieval() {
		return true;
	}

	@Override
	public boolean supportsBulkForeignKeyRetrieval() {
		return true;
	}

	@Override
	public NameSpaceIndexesInformation getIndexes(Identifier catalog, Identifier schema) {
		final String tableSchema = schema == null ? null : schema.getText();
		try ( var preparedStatement = getExtractionContext().getJdbcConnection().prepareStatement( getIndexesSql( tableSchema ) )) {
			if ( tableSchema != null ) {
				preparedStatement.setString( 1, tableSchema );
			}
			try ( var resultSet = preparedStatement.executeQuery() ) {
				return extractNameSpaceIndexesInformation( resultSet );
			}
		}
		catch (SQLException e) {
			throw convertSQLException( e,
					"Error while reading index information for namespace "
					+ new Namespace.Name( catalog, schema ) );
		}
	}

	private String getIndexesSql(String tableSchema) {
		final String sql = """
				select\
					current_database() as "TABLE_CAT",\
					n.nspname as "TABLE_SCHEM",\
					ct.relname as "TABLE_NAME",\
					not i.indisunique as "NON_UNIQUE",\
					null as "INDEX_QUALIFIER",\
					ci.relname as "INDEX_NAME",\
					case i.indisclustered\
						when true then 1\
						else\
							case am.amname\
								when 'hash' then 2\
								else 3\
							end\
					end as "TYPE",\
					ic.n as "ORDINAL_POSITION",\
					ci.reltuples as "CARDINALITY",\
					ci.relpages as "PAGES",\
					pg_catalog.pg_get_expr(i.indpred, i.indrelid) as "FILTER_CONDITION",\
					trim(both '"' from pg_catalog.pg_get_indexdef(ci.oid, ic.n, false)) as "COLUMN_NAME",\
					case am.amname\
						when 'btree' then\
							case i.indoption[ic.n - 1] & 1::smallint\
								when 1 then 'D'\
								else 'A'\
							end\
					end as "ASC_OR_DESC"
				from pg_catalog.pg_class ct
				join pg_catalog.pg_namespace n on (ct.relnamespace = n.oid)
				join pg_catalog.pg_index i on (ct.oid = i.indrelid)
				join pg_catalog.pg_class ci on (ci.oid = i.indexrelid)
				join pg_catalog.pg_am am on (ci.relam = am.oid)
				join information_schema._pg_expandarray(i.indkey) ic on 1=1
				""";
		return sql + (tableSchema == null ? "" : " where n.nspname = ?");
	}

	@Override
	public boolean supportsBulkIndexRetrieval() {
		return true;
	}

}

```


#### Error stacktrace:

```
scala.meta.internal.mtags.JavaToplevelMtags.quotedLiteral$1(JavaToplevelMtags.scala:176)
	scala.meta.internal.mtags.JavaToplevelMtags.parseToken$1(JavaToplevelMtags.scala:231)
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
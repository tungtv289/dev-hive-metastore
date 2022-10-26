//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hadoop.hive.metastore;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import javax.jdo.JDOException;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddForeignKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AddPrimaryKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterCatalogRequest;
import org.apache.hadoop.hive.metastore.api.AlterISchemaRequest;
import org.apache.hadoop.hive.metastore.api.AlterPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AlterPartitionsResponse;
import org.apache.hadoop.hive.metastore.api.AlterTableRequest;
import org.apache.hadoop.hive.metastore.api.AlterTableResponse;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreateCatalogRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DropCatalogRequest;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprResult;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GetRuntimeStatsRequest;
import org.apache.hadoop.hive.metastore.api.GetSerdeRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MapSchemaVersionToSerdeRequest;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionListComposingSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpecWithSharedSD;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionWithoutSD;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsResult;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.RenamePartitionRequest;
import org.apache.hadoop.hive.metastore.api.RenamePartitionResponse;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsResponse;
import org.apache.hadoop.hive.metastore.api.SetSchemaVersionStateRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableStatsResult;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.TruncateTableRequest;
import org.apache.hadoop.hive.metastore.api.TruncateTableResponse;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterPoolRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterPoolResponse;
import org.apache.hadoop.hive.metastore.api.WMAlterResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMAlterTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateOrDropTriggerToPoolMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateOrDropTriggerToPoolMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateOrUpdateMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateOrUpdateMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMCreatePoolRequest;
import org.apache.hadoop.hive.metastore.api.WMCreatePoolResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMDropMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMDropMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMDropPoolRequest;
import org.apache.hadoop.hive.metastore.api.WMDropPoolResponse;
import org.apache.hadoop.hive.metastore.api.WMDropResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMDropResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMDropTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMDropTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetAllResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetAllResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetTriggersForResourePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetTriggersForResourePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogResponse;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData._Fields;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.StatsUpdateMode;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.AlterCatalogEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterISchemaEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateISchemaEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DeletePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.DeleteTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.DropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.DropConstraintEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropISchemaEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAddSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterISchemaEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreAuthorizationCallEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateISchemaEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropISchemaEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreLoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.PreReadCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreReadDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreReadISchemaEvent;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.metastore.events.PreReadhSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.UpdateTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.metrics.JvmPauseMonitor;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy.PartitionIterator;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy.SimplePartitionWrapperIterator;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge.Server;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge.Server.ServerMode;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.CommonCliOptions;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.HdfsUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.LogUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.metastore.utils.HdfsUtils.HadoopFileStatus;
import org.apache.hadoop.hive.metastore.utils.LogUtils.LogInitializationException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.Warehouse.getCatalogQualifiedTableName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.*;

public class HiveMetaStore extends ThriftHiveMetastore {
    public static final Logger LOG = LoggerFactory.getLogger(HiveMetaStore.class);
    public static final String PARTITION_NUMBER_EXCEED_LIMIT_MSG = "Number of partitions scanned (=%d) on table '%s' exceeds limit (=%d). This is controlled on the metastore server by %s.";
    private static boolean isMetaStoreRemote = false;
    @VisibleForTesting
    static boolean TEST_TIMEOUT_ENABLED = false;
    @VisibleForTesting
    static long TEST_TIMEOUT_VALUE = -1L;
    private static ShutdownHookManager shutdownHookMgr;
    public static final String ADMIN = "admin";
    public static final String PUBLIC = "public";
    public static final char MM_WRITE_OPEN = 'o';
    public static final char MM_WRITE_COMMITTED = 'c';
    public static final char MM_WRITE_ABORTED = 'a';
    private static Server saslServer;
    private static MetastoreDelegationTokenManager delegationTokenManager;
    private static boolean useSasl;
    static final String NO_FILTER_STRING = "";
    static final int UNLIMITED_MAX_PARTITIONS = -1;
    private static AtomicInteger openConnections;
    private static int nextThreadId = 1000000;

    public HiveMetaStore() {
    }

    public static boolean isRenameAllowed(Database srcDB, Database destDB) {
        return srcDB.getName().equalsIgnoreCase(destDB.getName()) || !ReplChangeManager.isSourceOfReplication(srcDB) && !ReplChangeManager.isSourceOfReplication(destDB);
    }

    private static IHMSHandler newRetryingHMSHandler(IHMSHandler baseHandler, Configuration conf) throws MetaException {
        return newRetryingHMSHandler(baseHandler, conf, false);
    }

    private static IHMSHandler newRetryingHMSHandler(IHMSHandler baseHandler, Configuration conf, boolean local) throws MetaException {
        return RetryingHMSHandler.getProxy(conf, baseHandler, local);
    }

    static Iface newRetryingHMSHandler(String name, Configuration conf, boolean local) throws MetaException {
        HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler(name, conf, false);
        return RetryingHMSHandler.getProxy(conf, baseHandler, local);
    }

    public static void cancelDelegationToken(String tokenStrForm) throws IOException {
        delegationTokenManager.cancelDelegationToken(tokenStrForm);
    }

    public static String getDelegationToken(String owner, String renewer, String remoteAddr) throws IOException, InterruptedException {
        return delegationTokenManager.getDelegationToken(owner, renewer, remoteAddr);
    }

    public static boolean isMetaStoreRemote() {
        return isMetaStoreRemote;
    }

    public static long renewDelegationToken(String tokenStrForm) throws IOException {
        return delegationTokenManager.renewDelegationToken(tokenStrForm);
    }

    public static void main(String[] args) throws Throwable {
        Configuration conf = MetastoreConf.newMetastoreConf();
        shutdownHookMgr = ShutdownHookManager.get();
        HiveMetaStore.HiveMetastoreCli cli = new HiveMetaStore.HiveMetastoreCli(conf);
        cli.parse(args);
        boolean isCliVerbose = cli.isVerbose();
        Properties hiveconf = cli.addHiveconfToSystemProperties();

        try {
            if (System.getProperty("log4j.configurationFile") == null) {
                LogUtils.initHiveLog4j(conf);
            } else {
                LoggerContext context = (LoggerContext)LogManager.getContext(false);
                context.reconfigure();
            }
        } catch (LogInitializationException var10) {
            HiveMetaStore.HMSHandler.LOG.warn(var10.getMessage());
        }

        startupShutdownMessage(HiveMetaStore.class, args, LOG);

        try {
            String msg = "Starting hive metastore on port " + cli.port;
            HiveMetaStore.HMSHandler.LOG.info(msg);
            if (cli.isVerbose()) {
                System.err.println(msg);
            }

            Iterator var6 = hiveconf.entrySet().iterator();

            while(var6.hasNext()) {
                Entry<Object, Object> item = (Entry)var6.next();
                conf.set((String)item.getKey(), (String)item.getValue());
            }

            conf.set(ConfVars.THRIFT_URIS.getHiveName(), "");
            shutdownHookMgr.addShutdownHook(() -> {
                String shutdownMsg = "Shutting down hive metastore.";
                HiveMetaStore.HMSHandler.LOG.info(shutdownMsg);
                if (isCliVerbose) {
                    System.err.println(shutdownMsg);
                }

                if (MetastoreConf.getBoolVar(conf, ConfVars.METRICS_ENABLED)) {
                    try {
                        Metrics.shutdown();
                    } catch (Exception var4) {
                        LOG.error("error in Metrics deinit: " + var4.getClass().getName() + " " + var4.getMessage(), var4);
                    }
                }

                ThreadPool.shutdown();
            }, 10);
            if (MetastoreConf.getBoolVar(conf, ConfVars.METRICS_ENABLED)) {
                try {
                    Metrics.initialize(conf);
                } catch (Exception var9) {
                    LOG.error("error in Metrics init: " + var9.getClass().getName() + " " + var9.getMessage(), var9);
                }
            }

            Lock startLock = new ReentrantLock();
            Condition startCondition = startLock.newCondition();
            AtomicBoolean startedServing = new AtomicBoolean();
            startMetaStoreThreads(conf, startLock, startCondition, startedServing);
            startMetaStore(cli.getPort(), HadoopThriftAuthBridge.getBridge(), conf, startLock, startCondition, startedServing);
        } catch (Throwable var11) {
            HiveMetaStore.HMSHandler.LOG.error("Metastore Thrift Server threw an exception...", var11);
            throw var11;
        }
    }

    public static void startMetaStore(int port, HadoopThriftAuthBridge bridge) throws Throwable {
        startMetaStore(port, bridge, MetastoreConf.newMetastoreConf(), (Lock)null, (Condition)null, (AtomicBoolean)null);
    }

    public static void startMetaStore(int port, HadoopThriftAuthBridge bridge, Configuration conf) throws Throwable {
        startMetaStore(port, bridge, conf, (Lock)null, (Condition)null, (AtomicBoolean)null);
    }

    public static void startMetaStore(int port, HadoopThriftAuthBridge bridge, Configuration conf, Lock startLock, Condition startCondition, AtomicBoolean startedServing) throws Throwable {
        try {
            isMetaStoreRemote = true;
            long maxMessageSize = MetastoreConf.getLongVar(conf, ConfVars.SERVER_MAX_MESSAGE_SIZE);
            int minWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MIN_THREADS);
            int maxWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MAX_THREADS);
            boolean tcpKeepAlive = MetastoreConf.getBoolVar(conf, ConfVars.TCP_KEEP_ALIVE);
            boolean useFramedTransport = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
            boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
            boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
            useSasl = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_SASL);
            if (useSasl) {
                String kerberosName = SecurityUtil.getServerPrincipal(MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL), "0.0.0.0");
                String keyTabFile = MetastoreConf.getVar(conf, ConfVars.KERBEROS_KEYTAB_FILE);
                UserGroupInformation.loginUserFromKeytab(kerberosName, keyTabFile);
            }

            Object protocolFactory;
            Object inputProtoFactory;
            if (useCompactProtocol) {
                protocolFactory = new Factory();
                inputProtoFactory = new Factory(maxMessageSize, maxMessageSize);
            } else {
                protocolFactory = new org.apache.thrift.protocol.TBinaryProtocol.Factory();
                inputProtoFactory = new org.apache.thrift.protocol.TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize);
            }

            HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", conf, false);
            IHMSHandler handler = newRetryingHMSHandler(baseHandler, conf);
            Object processor;
            Object transFactory;
            if (useSasl) {
                if (useFramedTransport) {
                    throw new HiveMetaException("Framed transport is not supported with SASL enabled.");
                }

                saslServer = bridge.createServer(MetastoreConf.getVar(conf, ConfVars.KERBEROS_KEYTAB_FILE), MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL), MetastoreConf.getVar(conf, ConfVars.CLIENT_KERBEROS_PRINCIPAL));
                delegationTokenManager = new MetastoreDelegationTokenManager();
                delegationTokenManager.startDelegationTokenSecretManager(conf, baseHandler, ServerMode.METASTORE);
                saslServer.setSecretManager(delegationTokenManager.getSecretManager());
                transFactory = saslServer.createTransportFactory(MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
                processor = saslServer.wrapProcessor(new Processor(handler));
                LOG.info("Starting DB backed MetaStore Server in Secure Mode");
            } else if (MetastoreConf.getBoolVar(conf, ConfVars.EXECUTE_SET_UGI)) {
                transFactory = useFramedTransport ? new HiveMetaStore.ChainedTTransportFactory(new org.apache.thrift.transport.TFramedTransport.Factory(), new org.apache.hadoop.hive.metastore.security.TUGIContainingTransport.Factory()) : new org.apache.hadoop.hive.metastore.security.TUGIContainingTransport.Factory();
                processor = new TUGIBasedProcessor(handler);
                LOG.info("Starting DB backed MetaStore Server with SetUGI enabled");
            } else {
                transFactory = useFramedTransport ? new org.apache.thrift.transport.TFramedTransport.Factory() : new TTransportFactory();
                processor = new TSetIpAddressProcessor(handler);
                LOG.info("Starting DB backed MetaStore Server");
            }

            Object serverSocket;
            if (!useSSL) {
                serverSocket = SecurityUtils.getServerSocket((String)null, port);
            } else {
                String keyStorePath = MetastoreConf.getVar(conf, ConfVars.SSL_KEYSTORE_PATH).trim();
                if (keyStorePath.isEmpty()) {
                    throw new IllegalArgumentException(ConfVars.SSL_KEYSTORE_PATH.toString() + " Not configured for SSL connection");
                }

                String keyStorePassword = MetastoreConf.getPassword(conf, ConfVars.SSL_KEYSTORE_PASSWORD);
                List<String> sslVersionBlacklist = new ArrayList();
                String[] var24 = MetastoreConf.getVar(conf, ConfVars.SSL_PROTOCOL_BLACKLIST).split(",");
                int var25 = var24.length;

                for(int var26 = 0; var26 < var25; ++var26) {
                    String sslVersion = var24[var26];
                    sslVersionBlacklist.add(sslVersion);
                }

                serverSocket = SecurityUtils.getServerSSLSocket((String)null, port, keyStorePath, keyStorePassword, sslVersionBlacklist);
            }

            if (tcpKeepAlive) {
                serverSocket = new TServerSocketKeepAlive((TServerSocket)serverSocket);
            }

            openConnections = Metrics.getOrCreateGauge("open_connections");
            Args args = ((Args)((Args)((Args)((Args)(new Args((TServerTransport)serverSocket)).processor((TProcessor)processor)).transportFactory((TTransportFactory)transFactory)).protocolFactory((TProtocolFactory)protocolFactory)).inputProtocolFactory((TProtocolFactory)inputProtoFactory)).minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads);
            TServer tServer = new TThreadPoolServer(args);
            TServerEventHandler tServerEventHandler = new TServerEventHandler() {
                public void preServe() {
                }

                public ServerContext createContext(TProtocol tProtocol, TProtocol tProtocol1) {
                    HiveMetaStore.openConnections.incrementAndGet();
                    return null;
                }

                public void deleteContext(ServerContext serverContext, TProtocol tProtocol, TProtocol tProtocol1) {
                    HiveMetaStore.openConnections.decrementAndGet();
                    HiveMetaStore.cleanupRawStore();
                }

                public void processContext(ServerContext serverContext, TTransport tTransport, TTransport tTransport1) {
                }
            };
            tServer.setServerEventHandler(tServerEventHandler);
            HiveMetaStore.HMSHandler.LOG.info("Started the new metaserver on port [" + port + "]...");
            HiveMetaStore.HMSHandler.LOG.info("Options.minWorkerThreads = " + minWorkerThreads);
            HiveMetaStore.HMSHandler.LOG.info("Options.maxWorkerThreads = " + maxWorkerThreads);
            HiveMetaStore.HMSHandler.LOG.info("TCP keepalive = " + tcpKeepAlive);
            HiveMetaStore.HMSHandler.LOG.info("Enable SSL = " + useSSL);
            if (startLock != null) {
                signalOtherThreadsToStart(tServer, startLock, startCondition, startedServing);
            }

            tServer.serve();
        } catch (Throwable var28) {
            var28.printStackTrace();
            HiveMetaStore.HMSHandler.LOG.error(StringUtils.stringifyException(var28));
            throw var28;
        }
    }

    private static void cleanupRawStore() {
        boolean var4 = false;

        try {
            var4 = true;
            RawStore rs = HiveMetaStore.HMSHandler.getRawStore();
            if (rs != null) {
                HiveMetaStore.HMSHandler.logInfo("Cleaning up thread local RawStore...");
                rs.shutdown();
                var4 = false;
            } else {
                var4 = false;
            }
        } finally {
            if (var4) {
                HiveMetaStore.HMSHandler handler = (HiveMetaStore.HMSHandler)HiveMetaStore.HMSHandler.threadLocalHMSHandler.get();
                if (handler != null) {
                    handler.notifyMetaListenersOnShutDown();
                }

                HiveMetaStore.HMSHandler.threadLocalHMSHandler.remove();
                HiveMetaStore.HMSHandler.threadLocalConf.remove();
                HiveMetaStore.HMSHandler.threadLocalModifiedConfig.remove();
                HiveMetaStore.HMSHandler.removeRawStore();
                HiveMetaStore.HMSHandler.logInfo("Done cleaning up thread local RawStore");
            }
        }

        HiveMetaStore.HMSHandler handler = (HiveMetaStore.HMSHandler)HiveMetaStore.HMSHandler.threadLocalHMSHandler.get();
        if (handler != null) {
            handler.notifyMetaListenersOnShutDown();
        }

        HiveMetaStore.HMSHandler.threadLocalHMSHandler.remove();
        HiveMetaStore.HMSHandler.threadLocalConf.remove();
        HiveMetaStore.HMSHandler.threadLocalModifiedConfig.remove();
        HiveMetaStore.HMSHandler.removeRawStore();
        HiveMetaStore.HMSHandler.logInfo("Done cleaning up thread local RawStore");
    }

    private static void signalOtherThreadsToStart(final TServer server, final Lock startLock, final Condition startCondition, final AtomicBoolean startedServing) {
        Thread t = new Thread() {
            public void run() {
                do {
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException var6) {
                        HiveMetaStore.LOG.warn("Signalling thread was interrupted: " + var6.getMessage());
                    }
                } while(!server.isServing());

                startLock.lock();

                try {
                    startedServing.set(true);
                    startCondition.signalAll();
                } finally {
                    startLock.unlock();
                }

            }
        };
        t.start();
    }

    private static void startMetaStoreThreads(final Configuration conf, final Lock startLock, final Condition startCondition, final AtomicBoolean startedServing) {
        Thread t = new Thread() {
            public void run() {
                startLock.lock();

                try {
                    JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(conf);
                    pauseMonitor.start();
                } catch (Throwable var6) {
                    HiveMetaStore.LOG.warn("Could not initiate the JvmPauseMonitor thread. GCs and Pauses may not be warned upon.", var6);
                }

                try {
                    while(!startedServing.get()) {
                        startCondition.await();
                    }

                    HiveMetaStore.startCompactorInitiator(conf);
                    HiveMetaStore.startCompactorWorkers(conf);
                    HiveMetaStore.startCompactorCleaner(conf);
                    HiveMetaStore.startRemoteOnlyTasks(conf);
                    HiveMetaStore.startStatsUpdater(conf);
                } catch (Throwable var7) {
                    HiveMetaStore.LOG.error("Failure when starting the compactor, compactions may not happen, " + StringUtils.stringifyException(var7));
                } finally {
                    startLock.unlock();
                }

                ReplChangeManager.scheduleCMClearer(conf);
            }
        };
        t.setDaemon(true);
        t.setName("Metastore threads starter thread");
        t.start();
    }

    protected static void startStatsUpdater(Configuration conf) throws Exception {
        StatsUpdateMode mode = StatsUpdateMode.valueOf(MetastoreConf.getVar(conf, ConfVars.STATS_AUTO_UPDATE).toUpperCase());
        if (mode != StatsUpdateMode.NONE) {
            MetaStoreThread t = instantiateThread("org.apache.hadoop.hive.ql.stats.StatsUpdaterThread");
            initializeAndStartThread(t, conf);
        }
    }

    private static void startCompactorInitiator(Configuration conf) throws Exception {
        if (MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON)) {
            MetaStoreThread initiator = instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Initiator");
            initializeAndStartThread(initiator, conf);
        }

    }

    private static void startCompactorWorkers(Configuration conf) throws Exception {
        int numWorkers = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_WORKER_THREADS);

        for(int i = 0; i < numWorkers; ++i) {
            MetaStoreThread worker = instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Worker");
            initializeAndStartThread(worker, conf);
        }

    }

    private static void startCompactorCleaner(Configuration conf) throws Exception {
        if (MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON)) {
            MetaStoreThread cleaner = instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Cleaner");
            initializeAndStartThread(cleaner, conf);
        }

    }

    private static MetaStoreThread instantiateThread(String classname) throws Exception {
        Class<?> c = Class.forName(classname);
        Object o = c.newInstance();
        if (MetaStoreThread.class.isAssignableFrom(o.getClass())) {
            return (MetaStoreThread)o;
        } else {
            String s = classname + " is not an instance of MetaStoreThread.";
            LOG.error(s);
            throw new IOException(s);
        }
    }

    private static void initializeAndStartThread(MetaStoreThread thread, Configuration conf) throws MetaException {
        LOG.info("Starting metastore thread of type " + thread.getClass().getName());
        thread.setConf(conf);
        thread.setThreadId(nextThreadId++);
        thread.init(new AtomicBoolean(), new AtomicBoolean());
        thread.start();
    }

    private static void startRemoteOnlyTasks(Configuration conf) throws Exception {
        if (MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON)) {
            ThreadPool.initialize(conf);
            Collection<String> taskNames = MetastoreConf.getStringCollection(conf, ConfVars.TASK_THREADS_REMOTE_ONLY);
            Iterator var2 = taskNames.iterator();

            while(var2.hasNext()) {
                String taskName = (String)var2.next();
                MetastoreTaskThread task = (MetastoreTaskThread)JavaUtils.newInstance(JavaUtils.getClass(taskName, MetastoreTaskThread.class));
                task.setConf(conf);
                long freq = task.runFrequency(TimeUnit.MILLISECONDS);
                ThreadPool.getPool().scheduleAtFixedRate(task, freq, freq, TimeUnit.MILLISECONDS);
            }

        }
    }

    private static void startupShutdownMessage(Class<?> clazz, String[] args, Logger LOG) {
        String hostname = getHostname();
        String classname = clazz.getSimpleName();
        LOG.info(toStartupShutdownString("STARTUP_MSG: ", new String[]{"Starting " + classname, "  host = " + hostname, "  args = " + Arrays.asList(args), "  version = " + MetastoreVersionInfo.getVersion(), "  classpath = " + System.getProperty("java.class.path"), "  build = " + MetastoreVersionInfo.getUrl() + " -r " + MetastoreVersionInfo.getRevision() + "; compiled by '" + MetastoreVersionInfo.getUser() + "' on " + MetastoreVersionInfo.getDate()}));
        shutdownHookMgr.addShutdownHook(() -> {
            LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ", new String[]{"Shutting down " + classname + " at " + hostname}));
        }, 0);
    }

    private static String toStartupShutdownString(String prefix, String[] msg) {
        StringBuilder b = new StringBuilder(prefix);
        b.append("\n/************************************************************");
        String[] var3 = msg;
        int var4 = msg.length;

        for(int var5 = 0; var5 < var4; ++var5) {
            String s = var3[var5];
            b.append("\n").append(prefix).append(s);
        }

        b.append("\n************************************************************/");
        return b.toString();
    }

    private static String getHostname() {
        try {
            return "" + InetAddress.getLocalHost();
        } catch (UnknownHostException var1) {
            return "" + var1;
        }
    }

    public static class HiveMetastoreCli extends CommonCliOptions {
        private int port;

        HiveMetastoreCli(Configuration configuration) {
            super("hivemetastore", true);
            this.port = MetastoreConf.getIntVar(configuration, ConfVars.SERVER_PORT);
            Options var10000 = this.OPTIONS;
            OptionBuilder.hasArg();
            OptionBuilder.withArgName("port");
            OptionBuilder.withDescription("Hive Metastore port number, default:" + this.port);
            var10000.addOption(OptionBuilder.create('p'));
        }

        public void parse(String[] args) {
            super.parse(args);
            args = this.commandLine.getArgs();
            if (args.length > 0) {
                System.err.println("This usage has been deprecated, consider using the new command line syntax (run with -h to see usage information)");
                this.port = new Integer(args[0]);
            }

            if (this.commandLine.hasOption('p')) {
                this.port = Integer.parseInt(this.commandLine.getOptionValue('p'));
            } else {
                String metastorePort = System.getenv("METASTORE_PORT");
                if (metastorePort != null) {
                    this.port = Integer.parseInt(metastorePort);
                }
            }

        }

        public int getPort() {
            return this.port;
        }
    }

    public static class HMSHandler extends FacebookBase implements IHMSHandler {
        public static final Logger LOG;
        private final Configuration conf;
        private static final AtomicBoolean alwaysThreadsInitialized;
        private static String currentUrl;
        private FileMetadataManager fileMetadataManager;
        private PartitionExpressionProxy expressionProxy;
        private StorageSchemaReader storageSchemaReader;
        static AtomicInteger databaseCount;
        static AtomicInteger tableCount;
        static AtomicInteger partCount;
        private Warehouse wh;
        private static final ThreadLocal<RawStore> threadLocalMS;
        private static final ThreadLocal<TxnStore> threadLocalTxn;
        private static final ThreadLocal<Map<String, Context>> timerContexts;
        private static final ThreadLocal<Configuration> threadLocalConf;
        private static final ThreadLocal<HiveMetaStore.HMSHandler> threadLocalHMSHandler;
        private static final ThreadLocal<Map<String, String>> threadLocalModifiedConfig;
        private static ExecutorService threadPool;
        static final Logger auditLog;
        private static int nextSerialNum;
        private static ThreadLocal<Integer> threadLocalId;
        private static ThreadLocal<String> threadLocalIpAddress;
        private ClassLoader classLoader;
        private AlterHandler alterHandler;
        private List<MetaStorePreEventListener> preListeners;
        private List<MetaStoreEventListener> listeners;
        private List<TransactionalMetaStoreEventListener> transactionalListeners;
        private List<MetaStoreEndFunctionListener> endFunctionListeners;
        private List<MetaStoreInitListener> initListeners;
        private Pattern partitionValidationPattern;
        private final boolean isInTest;
        private static final Map<Long, ByteBuffer> EMPTY_MAP_FM1;
        private static final Map<Long, MetadataPpdResult> EMPTY_MAP_FM2;

        public static RawStore getRawStore() {
            return (RawStore)threadLocalMS.get();
        }

        static void removeRawStore() {
            threadLocalMS.remove();
        }

        private static void logAuditEvent(String cmd) {
            if (cmd != null) {
                UserGroupInformation ugi;
                try {
                    ugi = SecurityUtils.getUGI();
                } catch (Exception var3) {
                    throw new RuntimeException(var3);
                }

                String address = getIPAddress();
                if (address == null) {
                    address = "unknown-ip-addr";
                }

                auditLog.info("ugi={}\tip={}\tcmd={}\t", new Object[]{ugi.getUserName(), address, cmd});
            }
        }

        public static String getIPAddress() {
            if (HiveMetaStore.useSasl) {
                return HiveMetaStore.saslServer != null && HiveMetaStore.saslServer.getRemoteAddress() != null ? HiveMetaStore.saslServer.getRemoteAddress().getHostAddress() : null;
            } else {
                return getThreadLocalIpAddress();
            }
        }

        private void notifyMetaListeners(String key, String oldValue, String newValue) throws MetaException {
            Iterator var4 = this.listeners.iterator();

            while(var4.hasNext()) {
                MetaStoreEventListener listener = (MetaStoreEventListener)var4.next();
                listener.onConfigChange(new ConfigChangeEvent(this, key, oldValue, newValue));
            }

            if (this.transactionalListeners.size() > 0) {
                ConfigChangeEvent cce = new ConfigChangeEvent(this, key, oldValue, newValue);
                Iterator var7 = this.transactionalListeners.iterator();

                while(var7.hasNext()) {
                    MetaStoreEventListener transactionalListener = (MetaStoreEventListener)var7.next();
                    transactionalListener.onConfigChange(cce);
                }
            }

        }

        private void notifyMetaListenersOnShutDown() {
            Map<String, String> modifiedConf = (Map)threadLocalModifiedConfig.get();
            if (modifiedConf != null) {
                try {
                    Configuration conf = (Configuration)threadLocalConf.get();
                    if (conf == null) {
                        throw new MetaException("Unexpected: modifiedConf is non-null but conf is null");
                    }

                    Iterator var3 = modifiedConf.entrySet().iterator();

                    while(var3.hasNext()) {
                        Entry<String, String> entry = (Entry)var3.next();
                        String key = (String)entry.getKey();
                        String currVal = (String)entry.getValue();
                        String oldVal = conf.get(key);
                        if (!Objects.equals(oldVal, currVal)) {
                            this.notifyMetaListeners(key, oldVal, currVal);
                        }
                    }

                    logInfo("Meta listeners shutdown notification completed.");
                } catch (MetaException var8) {
                    LOG.error("Failed to notify meta listeners on shutdown: ", var8);
                }

            }
        }

        static void setThreadLocalIpAddress(String ipAddress) {
            threadLocalIpAddress.set(ipAddress);
        }

        static String getThreadLocalIpAddress() {
            return (String)threadLocalIpAddress.get();
        }

        @VisibleForTesting
        PartitionExpressionProxy getExpressionProxy() {
            return this.expressionProxy;
        }

        /** @deprecated */
        @Deprecated
        public static Integer get() {
            return (Integer)threadLocalId.get();
        }

        public int getThreadId() {
            return (Integer)threadLocalId.get();
        }

        public HMSHandler(String name) throws MetaException {
            this(name, MetastoreConf.newMetastoreConf(), true);
        }

        public HMSHandler(String name, Configuration conf) throws MetaException {
            this(name, conf, true);
        }

        public HMSHandler(String name, Configuration conf, boolean init) throws MetaException {
            super(name);
            this.classLoader = Thread.currentThread().getContextClassLoader();
            if (this.classLoader == null) {
                this.classLoader = Configuration.class.getClassLoader();
            }

            this.conf = conf;
            this.isInTest = MetastoreConf.getBoolVar(this.conf, ConfVars.HIVE_IN_TEST);
            if (threadPool == null) {
                Class var4 = HiveMetaStore.HMSHandler.class;
                synchronized(HiveMetaStore.HMSHandler.class) {
                    int numThreads = MetastoreConf.getIntVar(conf, ConfVars.FS_HANDLER_THREADS_COUNT);
                    threadPool = Executors.newFixedThreadPool(numThreads, (new ThreadFactoryBuilder()).setDaemon(true).setNameFormat("HMSHandler #%d").build());
                }
            }

            if (init) {
                this.init();
            }

        }

        /** @deprecated */
        @Deprecated
        public Configuration getHiveConf() {
            return this.conf;
        }

        public List<TransactionalMetaStoreEventListener> getTransactionalListeners() {
            return this.transactionalListeners;
        }

        public List<MetaStoreEventListener> getListeners() {
            return this.listeners;
        }

        public void init() throws MetaException {
            this.initListeners = MetaStoreUtils.getMetaStoreListeners(MetaStoreInitListener.class, this.conf, MetastoreConf.getVar(this.conf, ConfVars.INIT_HOOKS));
            Iterator var1 = this.initListeners.iterator();

            while(var1.hasNext()) {
                MetaStoreInitListener singleInitListener = (MetaStoreInitListener)var1.next();
                MetaStoreInitContext context = new MetaStoreInitContext();
                singleInitListener.onInit(context);
            }

            String alterHandlerName = MetastoreConf.getVar(this.conf, ConfVars.ALTER_HANDLER);
            this.alterHandler = (AlterHandler)ReflectionUtils.newInstance(JavaUtils.getClass(alterHandlerName, AlterHandler.class), this.conf);
            this.wh = new Warehouse(this.conf);
            Class var12 = HiveMetaStore.HMSHandler.class;
            synchronized(HiveMetaStore.HMSHandler.class) {
                if (currentUrl == null || !currentUrl.equals(MetaStoreInit.getConnectionURL(this.conf))) {
                    this.createDefaultDB();
                    this.createDefaultRoles();
                    this.addAdminUsers();
                    currentUrl = MetaStoreInit.getConnectionURL(this.conf);
                }
            }

            if (MetastoreConf.getBoolVar(this.conf, ConfVars.METRICS_ENABLED)) {
                LOG.info("Begin calculating metadata count metrics.");
                Metrics.initialize(this.conf);
                databaseCount = Metrics.getOrCreateGauge("total_count_dbs");
                tableCount = Metrics.getOrCreateGauge("total_count_tables");
                partCount = Metrics.getOrCreateGauge("total_count_partitions");
                this.updateMetrics();
            }

            this.preListeners = MetaStoreUtils.getMetaStoreListeners(MetaStorePreEventListener.class, this.conf, MetastoreConf.getVar(this.conf, ConfVars.PRE_EVENT_LISTENERS));
            this.preListeners.add(0, new TransactionalValidationListener(this.conf));
            this.listeners = MetaStoreUtils.getMetaStoreListeners(MetaStoreEventListener.class, this.conf, MetastoreConf.getVar(this.conf, ConfVars.EVENT_LISTENERS));
            this.listeners.add(new SessionPropertiesListener(this.conf));
            this.listeners.add(new AcidEventListener(this.conf));
            this.transactionalListeners = MetaStoreUtils.getMetaStoreListeners(TransactionalMetaStoreEventListener.class, this.conf, MetastoreConf.getVar(this.conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS));
            if (Metrics.getRegistry() != null) {
                this.listeners.add(new HMSMetricsListener(this.conf));
            }

            boolean doesAddEventsToNotificationLogTableFlag = false;
            Iterator var14 = this.transactionalListeners.iterator();

            while(var14.hasNext()) {
                MetaStoreEventListener listener = (MetaStoreEventListener)var14.next();
                if (listener.doesAddEventsToNotificationLogTable()) {
                    doesAddEventsToNotificationLogTableFlag = true;
                    break;
                }
            }

            if (this.conf.getBoolean(ConfVars.METASTORE_CACHE_CAN_USE_EVENT.getVarname(), false) && !doesAddEventsToNotificationLogTableFlag) {
                throw new MetaException("CahcedStore can not use events for invalidation as there is no  TransactionalMetaStoreEventListener to add events to notification table");
            } else {
                this.endFunctionListeners = MetaStoreUtils.getMetaStoreListeners(MetaStoreEndFunctionListener.class, this.conf, MetastoreConf.getVar(this.conf, ConfVars.END_FUNCTION_LISTENERS));
                String partitionValidationRegex = MetastoreConf.getVar(this.conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN);
                if (partitionValidationRegex != null && !partitionValidationRegex.isEmpty()) {
                    this.partitionValidationPattern = Pattern.compile(partitionValidationRegex);
                } else {
                    this.partitionValidationPattern = null;
                }

                if (alwaysThreadsInitialized.compareAndSet(false, true)) {
                    ThreadPool.initialize(this.conf);
                    Collection<String> taskNames = MetastoreConf.getStringCollection(this.conf, ConfVars.TASK_THREADS_ALWAYS);
                    Iterator var5 = taskNames.iterator();

                    while(var5.hasNext()) {
                        String taskName = (String)var5.next();
                        MetastoreTaskThread task = (MetastoreTaskThread)JavaUtils.newInstance(JavaUtils.getClass(taskName, MetastoreTaskThread.class));
                        task.setConf(this.conf);
                        long freq = task.runFrequency(TimeUnit.MILLISECONDS);
                        if (freq > 0L) {
                            ThreadPool.getPool().scheduleAtFixedRate(task, freq, freq, TimeUnit.MILLISECONDS);
                        }
                    }
                }

                this.expressionProxy = PartFilterExprUtil.createExpressionProxy(this.conf);
                this.fileMetadataManager = new FileMetadataManager(this.getMS(), this.conf);
            }
        }

        private static String addPrefix(String s) {
            return threadLocalId.get() + ": " + s;
        }

        private static void setHMSHandler(HiveMetaStore.HMSHandler handler) {
            if (threadLocalHMSHandler.get() == null) {
                threadLocalHMSHandler.set(handler);
            }

        }

        public void setConf(Configuration conf) {
            threadLocalConf.set(conf);
            RawStore ms = (RawStore)threadLocalMS.get();
            if (ms != null) {
                ms.setConf(conf);
            }

        }

        public Configuration getConf() {
            Configuration conf = (Configuration)threadLocalConf.get();
            if (conf == null) {
                conf = new Configuration(this.conf);
                threadLocalConf.set(conf);
            }

            return conf;
        }

        private Map<String, String> getModifiedConf() {
            Map<String, String> modifiedConf = (Map)threadLocalModifiedConfig.get();
            if (modifiedConf == null) {
                modifiedConf = new HashMap();
                threadLocalModifiedConfig.set(modifiedConf);
            }

            return (Map)modifiedConf;
        }

        public Warehouse getWh() {
            return this.wh;
        }

        public void setMetaConf(String key, String value) throws MetaException {
            ConfVars confVar = MetastoreConf.getMetaConf(key);
            if (confVar == null) {
                throw new MetaException("Invalid configuration key " + key);
            } else {
                try {
                    confVar.validate(value);
                } catch (IllegalArgumentException var7) {
                    throw new MetaException("Invalid configuration value " + value + " for key " + key + " by " + var7.getMessage());
                }

                Configuration configuration = this.getConf();
                String oldValue = MetastoreConf.get(configuration, key);
                Map<String, String> modifiedConf = this.getModifiedConf();
                if (!modifiedConf.containsKey(key)) {
                    modifiedConf.put(key, oldValue);
                }

                setHMSHandler(this);
                configuration.set(key, value);
                this.notifyMetaListeners(key, oldValue, value);
            }
        }

        public String getMetaConf(String key) throws MetaException {
            ConfVars confVar = MetastoreConf.getMetaConf(key);
            if (confVar == null) {
                throw new MetaException("Invalid configuration key " + key);
            } else {
                return this.getConf().get(key, confVar.getDefaultVal().toString());
            }
        }

        public RawStore getMS() throws MetaException {
            Configuration conf = this.getConf();
            return getMSForConf(conf);
        }

        public static RawStore getMSForConf(Configuration conf) throws MetaException {
            RawStore ms = (RawStore)threadLocalMS.get();
            if (ms == null) {
                ms = newRawStoreForConf(conf);

                try {
                    ms.verifySchema();
                } catch (MetaException var3) {
                    ms.shutdown();
                    throw var3;
                }

                threadLocalMS.set(ms);
                ms = (RawStore)threadLocalMS.get();
                LOG.info("Created RawStore: " + ms + " from thread id: " + Thread.currentThread().getId());
            }

            return ms;
        }

        public TxnStore getTxnHandler() {
            return getMsThreadTxnHandler(this.conf);
        }

        public static TxnStore getMsThreadTxnHandler(Configuration conf) {
            TxnStore txn = (TxnStore)threadLocalTxn.get();
            if (txn == null) {
                txn = TxnUtils.getTxnStore(conf);
                threadLocalTxn.set(txn);
            }

            return txn;
        }

        static RawStore newRawStoreForConf(Configuration conf) throws MetaException {
            Configuration newConf = new Configuration(conf);
            String rawStoreClassName = MetastoreConf.getVar(newConf, ConfVars.RAW_STORE_IMPL);
            LOG.info(addPrefix("Opening raw store with implementation class:" + rawStoreClassName));
            return RawStoreProxy.getProxy(newConf, conf, rawStoreClassName, (Integer)threadLocalId.get());
        }

        @VisibleForTesting
        public static void createDefaultCatalog(RawStore ms, Warehouse wh) throws MetaException, InvalidOperationException {
            try {
                Catalog defaultCat = ms.getCatalog("hive");
                if (defaultCat != null && defaultCat.getLocationUri().equals("TBD")) {
                    LOG.info("Setting location of default catalog, as it hasn't been done after upgrade");
                    defaultCat.setLocationUri(wh.getWhRoot().toString());
                    ms.alterCatalog(defaultCat.getName(), defaultCat);
                }
            } catch (NoSuchObjectException var4) {
                Catalog cat = new Catalog("hive", wh.getWhRoot().toString());
                cat.setDescription("Default catalog, for Hive");
                ms.createCatalog(cat);
            }

        }

        private void createDefaultDB_core(RawStore ms) throws MetaException, InvalidObjectException {
            try {
                ms.getDatabase("hive", "default");
            } catch (NoSuchObjectException var4) {
                Database db = new Database("default", "Default Hive database", this.wh.getDefaultDatabasePath("default").toString(), (Map)null);
                db.setOwnerName("public");
                db.setOwnerType(PrincipalType.ROLE);
                db.setCatalogName("hive");
                ms.createDatabase(db);
            }

        }

        private void createDefaultDB() throws MetaException {
            try {
                RawStore ms = this.getMS();
                createDefaultCatalog(ms, this.wh);
                this.createDefaultDB_core(ms);
            } catch (JDOException var4) {
                LOG.warn("Retrying creating default database after error: " + var4.getMessage(), var4);

                try {
                    this.createDefaultDB_core(this.getMS());
                } catch (InvalidObjectException var3) {
                    throw new MetaException(var3.getMessage());
                }
            } catch (InvalidOperationException | InvalidObjectException var5) {
                throw new MetaException(var5.getMessage());
            }

        }

        private void createDefaultRoles() throws MetaException {
            try {
                this.createDefaultRoles_core();
            } catch (JDOException var2) {
                LOG.warn("Retrying creating default roles after error: " + var2.getMessage(), var2);
                this.createDefaultRoles_core();
            }

        }

        private void createDefaultRoles_core() throws MetaException {
            RawStore ms = this.getMS();

            try {
                ms.addRole("admin", "admin");
            } catch (InvalidObjectException var8) {
                LOG.debug("admin role already exists", var8);
            } catch (NoSuchObjectException var9) {
                LOG.warn("Unexpected exception while adding admin roles", var9);
            }

            LOG.info("Added admin role in metastore");

            try {
                ms.addRole("public", "public");
            } catch (InvalidObjectException var6) {
                LOG.debug("public role already exists", var6);
            } catch (NoSuchObjectException var7) {
                LOG.warn("Unexpected exception while adding public roles", var7);
            }

            LOG.info("Added public role in metastore");
            PrivilegeBag privs = new PrivilegeBag();
            privs.addToPrivileges(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.GLOBAL, (String)null, (String)null, (List)null, (String)null), "admin", PrincipalType.ROLE, new PrivilegeGrantInfo("All", 0, "admin", PrincipalType.ROLE, true), "SQL"));

            try {
                ms.grantPrivileges(privs);
            } catch (InvalidObjectException var4) {
                LOG.debug("Failed while granting global privs to admin", var4);
            } catch (NoSuchObjectException var5) {
                LOG.warn("Failed while granting global privs to admin", var5);
            }

        }

        private void addAdminUsers() throws MetaException {
            try {
                this.addAdminUsers_core();
            } catch (JDOException var2) {
                LOG.warn("Retrying adding admin users after error: " + var2.getMessage(), var2);
                this.addAdminUsers_core();
            }

        }

        private void addAdminUsers_core() throws MetaException {
            String userStr = MetastoreConf.getVar(this.conf, ConfVars.USERS_IN_ADMIN_ROLE, "").trim();
            if (userStr.isEmpty()) {
                LOG.info("No user is added in admin role, since config is empty");
            } else {
                Iterator<String> users = Splitter.on(",").trimResults().omitEmptyStrings().split(userStr).iterator();
                if (!users.hasNext()) {
                    LOG.info("No user is added in admin role, since config value " + userStr + " is in incorrect format. We accept comma separated list of users.");
                } else {
                    RawStore ms = this.getMS();

                    Role adminRole;
                    try {
                        adminRole = ms.getRole("admin");
                    } catch (NoSuchObjectException var9) {
                        LOG.error("Failed to retrieve just added admin role", var9);
                        return;
                    }

                    while(users.hasNext()) {
                        String userName = (String)users.next();

                        try {
                            ms.grantRole(adminRole, userName, PrincipalType.USER, "admin", PrincipalType.ROLE, true);
                            LOG.info("Added " + userName + " to admin role");
                        } catch (NoSuchObjectException var7) {
                            LOG.error("Failed to add " + userName + " in admin role", var7);
                        } catch (InvalidObjectException var8) {
                            LOG.debug(userName + " already in admin role", var8);
                        }
                    }

                }
            }
        }

        private static void logInfo(String m) {
            LOG.info(((Integer)threadLocalId.get()).toString() + ": " + m);
            logAuditEvent(m);
        }

        private String startFunction(String function, String extraLogInfo) {
            this.incrementCounter(function);
            logInfo((getThreadLocalIpAddress() == null ? "" : "source:" + getThreadLocalIpAddress() + " ") + function + extraLogInfo);
            Timer timer = Metrics.getOrCreateTimer("api_" + function);
            if (timer != null) {
                ((Map)timerContexts.get()).put(function, timer.time());
            }

            Counter counter = Metrics.getOrCreateCounter("active_calls_" + function);
            if (counter != null) {
                counter.inc();
            }

            return function;
        }

        private String startFunction(String function) {
            return this.startFunction(function, "");
        }

        private void startTableFunction(String function, String catName, String db, String tbl) {
            this.startFunction(function, " : tbl=" + Warehouse.getCatalogQualifiedTableName(catName, db, tbl));
        }

        private void startMultiTableFunction(String function, String db, List<String> tbls) {
            String tableNames = org.apache.commons.lang.StringUtils.join(tbls, ",");
            this.startFunction(function, " : db=" + db + " tbls=" + tableNames);
        }

        private void startPartitionFunction(String function, String cat, String db, String tbl, List<String> partVals) {
            this.startFunction(function, " : tbl=" + Warehouse.getCatalogQualifiedTableName(cat, db, tbl) + "[" + org.apache.commons.lang.StringUtils.join(partVals, ",") + "]");
        }

        private void startPartitionFunction(String function, String catName, String db, String tbl, Map<String, String> partName) {
            this.startFunction(function, " : tbl=" + Warehouse.getCatalogQualifiedTableName(catName, db, tbl) + "partition=" + partName);
        }

        private void endFunction(String function, boolean successful, Exception e) {
            this.endFunction(function, successful, e, (String)null);
        }

        private void endFunction(String function, boolean successful, Exception e, String inputTableName) {
            this.endFunction(function, new MetaStoreEndFunctionContext(successful, e, inputTableName));
        }

        private void endFunction(String function, MetaStoreEndFunctionContext context) {
            Context timerContext = (Context)((Map)timerContexts.get()).remove(function);
            if (timerContext != null) {
                timerContext.close();
            }

            Counter counter = Metrics.getOrCreateCounter("active_calls_" + function);
            if (counter != null) {
                counter.dec();
            }

            Iterator var5 = this.endFunctionListeners.iterator();

            while(var5.hasNext()) {
                MetaStoreEndFunctionListener listener = (MetaStoreEndFunctionListener)var5.next();
                listener.onEndFunction(function, context);
            }

        }

        public fb_status getStatus() {
            return fb_status.ALIVE;
        }

        public void shutdown() {
            HiveMetaStore.cleanupRawStore();
            PerfLogger.getPerfLogger(false).cleanupPerfLogMetrics();
        }

        public AbstractMap<String, Long> getCounters() {
            AbstractMap<String, Long> counters = super.getCounters();
            if (this.endFunctionListeners != null) {
                Iterator var2 = this.endFunctionListeners.iterator();

                while(var2.hasNext()) {
                    MetaStoreEndFunctionListener listener = (MetaStoreEndFunctionListener)var2.next();
                    listener.exportCounters(counters);
                }
            }

            return counters;
        }

        public void create_catalog(CreateCatalogRequest rqst) throws AlreadyExistsException, InvalidObjectException, MetaException {
            Catalog catalog = rqst.getCatalog();
            this.startFunction("create_catalog", ": " + catalog.toString());
            boolean success = false;
            Exception ex = null;

            try {
                try {
                    this.getMS().getCatalog(catalog.getName());
                    throw new AlreadyExistsException("Catalog " + catalog.getName() + " already exists");
                } catch (NoSuchObjectException var20) {
                    if (!MetaStoreUtils.validateName(catalog.getName(), (Configuration)null)) {
                        throw new InvalidObjectException(catalog.getName() + " is not a valid catalog name");
                    }
                }

                if (catalog.getLocationUri() == null) {
                    throw new InvalidObjectException("You must specify a path for the catalog");
                }

                RawStore ms = this.getMS();
                Path catPath = new Path(catalog.getLocationUri());
                boolean madeDir = false;
                Map transactionalListenersResponses = Collections.emptyMap();

                try {
                    this.firePreEvent(new PreCreateCatalogEvent(this, catalog));
                    if (!this.wh.isDir(catPath)) {
                        if (!this.wh.mkdirs(catPath)) {
                            throw new MetaException("Unable to create catalog path " + catPath + ", failed to create catalog " + catalog.getName());
                        }

                        madeDir = true;
                    }

                    ms.openTransaction();
                    ms.createCatalog(catalog);
                    Database db = new Database("default", "Default database for catalog " + catalog.getName(), catalog.getLocationUri(), Collections.emptyMap());
                    db.setCatalogName(catalog.getName());
                    this.create_database_core(ms, db);
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.CREATE_CATALOG, new CreateCatalogEvent(true, this, catalog));
                    }

                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                        if (madeDir) {
                            this.wh.deleteDir(catPath, true, false, false);
                        }
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.CREATE_CATALOG, new CreateCatalogEvent(success, this, catalog), (EnvironmentContext)null, transactionalListenersResponses, ms);
                    }

                }

                success = true;
            } catch (InvalidObjectException | MetaException | AlreadyExistsException var22) {
                ex = var22;
                throw var22;
            } finally {
                this.endFunction("create_catalog", success, ex);
            }

        }

        public void alter_catalog(AlterCatalogRequest rqst) throws TException {
            this.startFunction("alter_catalog " + rqst.getName());
            boolean success = false;
            Exception ex = null;
            RawStore ms = this.getMS();
            Map<String, String> transactionalListenersResponses = Collections.emptyMap();
            GetCatalogResponse oldCat = null;

            try {
                oldCat = this.get_catalog(new GetCatalogRequest(rqst.getName()));

                assert oldCat != null && oldCat.getCatalog() != null;

                this.firePreEvent(new PreAlterCatalogEvent(oldCat.getCatalog(), rqst.getNewCat(), this));
                ms.openTransaction();
                ms.alterCatalog(rqst.getName(), rqst.getNewCat());
                if (!this.transactionalListeners.isEmpty()) {
                    transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ALTER_CATALOG, new AlterCatalogEvent(oldCat.getCatalog(), rqst.getNewCat(), true, this));
                }

                success = ms.commitTransaction();
            } catch (NoSuchObjectException | MetaException var11) {
                ex = var11;
                throw var11;
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                }

                if (null != oldCat && !this.listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALTER_CATALOG, new AlterCatalogEvent(oldCat.getCatalog(), rqst.getNewCat(), success, this), (EnvironmentContext)null, transactionalListenersResponses, ms);
                }

                this.endFunction("alter_catalog", success, ex);
            }

        }

        public GetCatalogResponse get_catalog(GetCatalogRequest rqst) throws NoSuchObjectException, TException {
            String catName = rqst.getName();
            this.startFunction("get_catalog", ": " + catName);
            Catalog cat = null;
            Exception ex = null;

            GetCatalogResponse var5;
            try {
                cat = this.getMS().getCatalog(catName);
                this.firePreEvent(new PreReadCatalogEvent(this, cat));
                var5 = new GetCatalogResponse(cat);
            } catch (NoSuchObjectException | MetaException var9) {
                ex = var9;
                throw var9;
            } finally {
                this.endFunction("get_database", cat != null, ex);
            }

            return var5;
        }

        public GetCatalogsResponse get_catalogs() throws MetaException {
            this.startFunction("get_catalogs");
            List<String> ret = null;
            MetaException ex = null;

            try {
                ret = this.getMS().getCatalogs();
            } catch (MetaException var7) {
                ex = var7;
                throw var7;
            } finally {
                this.endFunction("get_catalog", ret != null, ex);
            }

            return new GetCatalogsResponse(ret == null ? Collections.emptyList() : ret);
        }

        public void drop_catalog(DropCatalogRequest rqst) throws NoSuchObjectException, InvalidOperationException, MetaException {
            String catName = rqst.getName();
            this.startFunction("drop_catalog", ": " + catName);
            if ("hive".equalsIgnoreCase(catName)) {
                this.endFunction("drop_catalog", false, (Exception)null);
                throw new MetaException("Can not drop hive catalog");
            } else {
                boolean success = false;
                Object ex = null;

                try {
                    this.dropCatalogCore(catName);
                    success = true;
                } catch (InvalidOperationException | MetaException | NoSuchObjectException var10) {
                    ex = var10;
                    throw var10;
                } catch (Exception var11) {
                    ex = var11;
                    throw newMetaException(var11);
                } finally {
                    this.endFunction("drop_catalog", success, (Exception)ex);
                }

            }
        }

        private void dropCatalogCore(String catName) throws MetaException, NoSuchObjectException, InvalidOperationException {
            boolean success = false;
            Catalog cat = null;
            Map<String, String> transactionalListenerResponses = Collections.emptyMap();
            RawStore ms = this.getMS();

            try {
                ms.openTransaction();
                cat = ms.getCatalog(catName);
                this.firePreEvent(new PreDropCatalogEvent(this, cat));
                List<String> allDbs = this.get_databases(MetaStoreUtils.prependNotNullCatToDbName(catName, (String)null));
                if (allDbs != null && !allDbs.isEmpty()) {
                    if (allDbs.size() != 1 || !((String)allDbs.get(0)).equals("default")) {
                        throw new InvalidOperationException("There are non-default databases in the catalog " + catName + " so it cannot be dropped.");
                    }

                    try {
                        this.drop_database_core(ms, catName, "default", true, false);
                    } catch (InvalidOperationException var13) {
                        throw new InvalidOperationException("There are still objects in the default database for catalog " + catName);
                    } catch (IOException | InvalidInputException | InvalidObjectException var14) {
                        MetaException me = new MetaException("Error attempt to drop default database for catalog " + catName);
                        me.initCause(var14);
                        throw me;
                    }
                }

                ms.dropCatalog(catName);
                if (!this.transactionalListeners.isEmpty()) {
                    transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.DROP_CATALOG, new DropCatalogEvent(true, this, cat));
                }

                success = ms.commitTransaction();
            } finally {
                if (success) {
                    this.wh.deleteDir(this.wh.getDnsPath(new Path(cat.getLocationUri())), false, false, false);
                } else {
                    ms.rollbackTransaction();
                }

                if (!this.listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.DROP_CATALOG, new DropCatalogEvent(success, this, cat), (EnvironmentContext)null, transactionalListenerResponses, ms);
                }

            }

        }

        private void create_database_core(RawStore ms, final Database db) throws AlreadyExistsException, InvalidObjectException, MetaException {
            if (!MetaStoreUtils.validateName(db.getName(), (Configuration)null)) {
                throw new InvalidObjectException(db.getName() + " is not a valid database name");
            } else {
                Catalog cat = null;

                try {
                    cat = this.getMS().getCatalog(db.getCatalogName());
                } catch (NoSuchObjectException var25) {
                    LOG.error("No such catalog " + db.getCatalogName());
                    throw new InvalidObjectException("No such catalog " + db.getCatalogName());
                }

                final Path dbPath = this.wh.determineDatabasePath(cat, db);
                final Path dbExternalPath = this.wh.determineDatabaseExternalPath(db);
                db.setLocationUri(dbPath.toString());
                boolean success = false;
                boolean madeManagedDir = false;
                boolean madeExternalDir = false;
                Map transactionalListenersResponses = Collections.emptyMap();

                try {
                    this.firePreEvent(new PreCreateDatabaseEvent(db, this));
                    if (db.getCatalogName() != null && !db.getCatalogName().equals("hive")) {
                        if (!this.wh.isDir(dbPath)) {
                            LOG.debug("Creating database path " + dbPath);
                            if (!this.wh.mkdirs(dbPath)) {
                                throw new MetaException("Unable to create database path " + dbPath + ", failed to create database " + db.getName());
                            }

                            madeManagedDir = true;
                        }
                    } else {
                        try {
                            madeManagedDir = (Boolean)UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Boolean>() {
                                public Boolean run() throws MetaException {
                                    if (!HMSHandler.this.wh.isDir(dbPath)) {
                                        HiveMetaStore.HMSHandler.LOG.info("Creating database path in managed directory " + dbPath);
                                        if (!HMSHandler.this.wh.mkdirs(dbPath)) {
                                            throw new MetaException("Unable to create database managed path " + dbPath + ", failed to create database " + db.getName());
                                        } else {
                                            return true;
                                        }
                                    } else {
                                        return false;
                                    }
                                }
                            });
                            if (madeManagedDir) {
                                LOG.info("Created database path in managed directory " + dbPath);
                            }
                        } catch (InterruptedException | IOException var24) {
                            throw new MetaException("Unable to create database managed directory " + dbPath + ", failed to create database " + db.getName());
                        }

                        if (this.wh.hasExternalWarehouseRoot() && dbExternalPath != null) {
                            try {
                                madeExternalDir = (Boolean)UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Boolean>() {
                                    public Boolean run() throws MetaException {
                                        if (!HMSHandler.this.wh.isDir(dbExternalPath)) {
                                            HiveMetaStore.HMSHandler.LOG.info("Creating database path in external directory " + dbExternalPath);
                                            return HMSHandler.this.wh.mkdirs(dbExternalPath);
                                        } else {
                                            return false;
                                        }
                                    }
                                });
                                if (madeExternalDir) {
                                    LOG.info("Created database path in external directory " + dbPath);
                                } else {
                                    LOG.warn("Failed to create external path " + dbExternalPath + " for database " + db.getName() + ". This may result in access not being allowed if the " + "StorageBasedAuthorizationProvider is enabled ");
                                }
                            } catch (InterruptedException | UndeclaredThrowableException | IOException var23) {
                                LOG.warn("Failed to create external path " + dbExternalPath + " for database " + db.getName() + ". This may result in access not being allowed if the " + "StorageBasedAuthorizationProvider is enabled: " + var23.getMessage());
                            }
                        } else {
                            LOG.info("Database external path won't be created since the externaldirectory is not defined");
                        }
                    }

                    ms.openTransaction();
                    ms.createDatabase(db);
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.CREATE_DATABASE, new CreateDatabaseEvent(db, true, this));
                    }

                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                        if (db.getCatalogName() != null && !db.getCatalogName().equals("hive")) {
                            if (madeManagedDir) {
                                this.wh.deleteDir(dbPath, true, db);
                            }
                        } else {
                            if (madeManagedDir) {
                                try {
                                    class NamelessClass_4 implements PrivilegedExceptionAction<Void> {
                                        NamelessClass_4() {
                                        }

                                        public Void run() throws Exception {
                                            HMSHandler.this.wh.deleteDir(dbPath, true, db);
                                            return null;
                                        }
                                    }

                                    UserGroupInformation.getLoginUser().doAs(new NamelessClass_4());
                                } catch (InterruptedException | IOException var22) {
                                    LOG.error("Couldn't delete managed directory " + dbPath + " after " + "it was created for database " + db.getName() + " " + var22.getMessage());
                                }
                            }

                            if (madeExternalDir) {
                                try {
                                    class NamelessClass_3 implements PrivilegedExceptionAction<Void> {
                                        NamelessClass_3() {
                                        }

                                        public Void run() throws Exception {
                                            HMSHandler.this.wh.deleteDir(dbExternalPath, true, db);
                                            return null;
                                        }
                                    }

                                    UserGroupInformation.getCurrentUser().doAs(new NamelessClass_3());
                                } catch (InterruptedException | IOException var21) {
                                    LOG.error("Couldn't delete external directory " + dbExternalPath + " after " + "it was created for database " + db.getName() + " " + var21.getMessage());
                                }
                            }
                        }
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.CREATE_DATABASE, new CreateDatabaseEvent(db, success, this), (EnvironmentContext)null, transactionalListenersResponses, ms);
                    }

                }

            }
        }

        public void create_database(Database db) throws AlreadyExistsException, InvalidObjectException, MetaException {
            this.startFunction("create_database", ": " + db.toString());
            boolean success = false;
            Exception ex = null;
            if (!db.isSetCatalogName()) {
                db.setCatalogName(MetaStoreUtils.getDefaultCatalog(this.conf));
            }

            try {
                try {
                    if (null != this.get_database_core(db.getCatalogName(), db.getName())) {
                        throw new AlreadyExistsException("Database " + db.getName() + " already exists");
                    }
                } catch (NoSuchObjectException var11) {
                }

                if (HiveMetaStore.TEST_TIMEOUT_ENABLED) {
                    try {
                        Thread.sleep(HiveMetaStore.TEST_TIMEOUT_VALUE);
                    } catch (InterruptedException var10) {
                    }

                    Deadline.checkTimeout();
                }

                this.create_database_core(this.getMS(), db);
                success = true;
            } catch (Exception var12) {
                ex = var12;
                if (var12 instanceof MetaException) {
                    throw (MetaException)var12;
                }

                if (var12 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var12;
                }

                if (var12 instanceof AlreadyExistsException) {
                    throw (AlreadyExistsException)var12;
                }

                throw newMetaException(var12);
            } finally {
                this.endFunction("create_database", success, ex);
            }

        }

        public Database get_database(String name) throws NoSuchObjectException, MetaException {
            this.startFunction("get_database", ": " + name);
            Database db = null;
            Exception ex = null;

            try {
                String[] parsedDbName = MetaStoreUtils.parseDbName(name, this.conf);
                db = this.get_database_core(parsedDbName[0], parsedDbName[1]);
                this.firePreEvent(new PreReadDatabaseEvent(db, this));
            } catch (NoSuchObjectException | MetaException var8) {
                ex = var8;
                throw var8;
            } finally {
                this.endFunction("get_database", db != null, ex);
            }

            return db;
        }

        public Database get_database_core(String catName, String name) throws NoSuchObjectException, MetaException {
            Database db = null;
            if (name == null) {
                throw new MetaException("Database name cannot be null.");
            } else {
                try {
                    db = this.getMS().getDatabase(catName, name);
                    return db;
                } catch (NoSuchObjectException | MetaException var5) {
                    throw var5;
                } catch (Exception var6) {
                    assert var6 instanceof RuntimeException;

                    throw (RuntimeException)var6;
                }
            }
        }

        public void alter_database(String dbName, Database newDB) throws TException {
            this.startFunction("alter_database " + dbName);
            boolean success = false;
            Exception ex = null;
            RawStore ms = this.getMS();
            Database oldDB = null;
            Map<String, String> transactionalListenersResponses = Collections.emptyMap();
            if (newDB.getLocationUri() != null) {
                newDB.setLocationUri(this.wh.getDnsPath(new Path(newDB.getLocationUri())).toString());
            }

            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);

            try {
                oldDB = this.get_database_core(parsedDbName[0], parsedDbName[1]);
                if (oldDB == null) {
                    throw new MetaException("Could not alter database \"" + parsedDbName[1] + "\". Could not retrieve old definition.");
                }

                this.firePreEvent(new PreAlterDatabaseEvent(oldDB, newDB, this));
                ms.openTransaction();
                ms.alterDatabase(parsedDbName[0], parsedDbName[1], newDB);
                if (!this.transactionalListeners.isEmpty()) {
                    transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ALTER_DATABASE, new AlterDatabaseEvent(oldDB, newDB, true, this));
                }

                success = ms.commitTransaction();
            } catch (NoSuchObjectException | MetaException var13) {
                ex = var13;
                throw var13;
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                }

                if (null != oldDB && !this.listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALTER_DATABASE, new AlterDatabaseEvent(oldDB, newDB, success, this), (EnvironmentContext)null, transactionalListenersResponses, ms);
                }

                this.endFunction("alter_database", success, ex);
            }

        }

        private void drop_database_core(RawStore ms, String catName, String name, boolean deleteData, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException, IOException, InvalidObjectException, InvalidInputException {
            boolean success = false;
            Database db = null;
            List<Path> tablePaths = new ArrayList<>();
            List<Path> partitionPaths = new ArrayList<>();
            Map<String, String> transactionalListenerResponses = Collections.emptyMap();
            if (name == null) {
                throw new MetaException("Database name cannot be null.");
            }
            try {
                ms.openTransaction();
                db = ms.getDatabase(catName, name);

                firePreEvent(new PreDropDatabaseEvent(db, this));
                String catPrependedName = MetaStoreUtils.prependCatalogToDbName(catName, name, conf);

                Set<String> uniqueTableNames = new HashSet<>(get_all_tables(catPrependedName));
                List<String> allFunctions = get_functions(catPrependedName, "*");

                if (!cascade) {
                    if (!uniqueTableNames.isEmpty()) {
                        throw new InvalidOperationException(
                                "Database " + db.getName() + " is not empty. One or more tables exist.");
                    }
                    if (!allFunctions.isEmpty()) {
                        throw new InvalidOperationException(
                                "Database " + db.getName() + " is not empty. One or more functions exist.");
                    }
                }
                Path path = new Path(db.getLocationUri()).getParent();
                if (!wh.isWritable(path)) {
                    throw new MetaException("Database not dropped since " +
                            path + " is not writable by " +
                            SecurityUtils.getUser());
                }

                Path databasePath = wh.getDnsPath(wh.getDatabasePath(db));

                // drop any functions before dropping db
                for (String funcName : allFunctions) {
                    drop_function(catPrependedName, funcName);
                }

                final int tableBatchSize = MetastoreConf.getIntVar(conf,
                        ConfVars.BATCH_RETRIEVE_MAX);

                // First pass will drop the materialized views
                List<String> materializedViewNames = get_tables_by_type(name, ".*", TableType.MATERIALIZED_VIEW.toString());
                int startIndex = 0;
                // retrieve the tables from the metastore in batches to alleviate memory constraints
                while (startIndex < materializedViewNames.size()) {
                    int endIndex = Math.min(startIndex + tableBatchSize, materializedViewNames.size());

                    List<Table> materializedViews;
                    try {
                        materializedViews = ms.getTableObjectsByName(catName, name, materializedViewNames.subList(startIndex, endIndex));
                    } catch (UnknownDBException e) {
                        throw new MetaException(e.getMessage());
                    }

                    if (materializedViews != null && !materializedViews.isEmpty()) {
                        for (Table materializedView : materializedViews) {
                            if (materializedView.getSd().getLocation() != null) {
                                Path materializedViewPath = wh.getDnsPath(new Path(materializedView.getSd().getLocation()));
                                if (!wh.isWritable(materializedViewPath.getParent())) {
                                    throw new MetaException("Database metadata not deleted since table: " +
                                            materializedView.getTableName() + " has a parent location " + materializedViewPath.getParent() +
                                            " which is not writable by " + SecurityUtils.getUser());
                                }

                                if (!isSubdirectory(databasePath, materializedViewPath)) {
                                    tablePaths.add(materializedViewPath);
                                }
                            }
                            // Drop the materialized view but not its data
                            drop_table(name, materializedView.getTableName(), false);
                            // Remove from all tables
                            uniqueTableNames.remove(materializedView.getTableName());
                        }
                    }
                    startIndex = endIndex;
                }

                // drop tables before dropping db
                List<String> allTables = new ArrayList<>(uniqueTableNames);
                startIndex = 0;
                // retrieve the tables from the metastore in batches to alleviate memory constraints
                while (startIndex < allTables.size()) {
                    int endIndex = Math.min(startIndex + tableBatchSize, allTables.size());

                    List<Table> tables;
                    try {
                        tables = ms.getTableObjectsByName(catName, name, allTables.subList(startIndex, endIndex));
                    } catch (UnknownDBException e) {
                        throw new MetaException(e.getMessage());
                    }

                    if (tables != null && !tables.isEmpty()) {
                        for (Table table : tables) {
                            // If the table is not external and it might not be in a subdirectory of the database
                            // add it's locations to the list of paths to delete
                            Path tablePath = null;
                            if (table.getSd().getLocation() != null && !isExternal(table)) {
                                tablePath = wh.getDnsPath(new Path(table.getSd().getLocation()));
                                if (!wh.isWritable(tablePath.getParent())) {
                                    throw new MetaException("Database metadata not deleted since table: " +
                                            table.getTableName() + " has a parent location " + tablePath.getParent() +
                                            " which is not writable by " + SecurityUtils.getUser());
                                }

                                if (!isSubdirectory(databasePath, tablePath)) {
                                    tablePaths.add(tablePath);
                                }
                            }

                            // For each partition in each table, drop the partitions and get a list of
                            // partitions' locations which might need to be deleted
                            partitionPaths = dropPartitionsAndGetLocations(ms, catName, name, table.getTableName(),
                                    tablePath, table.getPartitionKeys(), deleteData && !isExternal(table));

                            // Drop the table but not its data
                            drop_table(MetaStoreUtils.prependCatalogToDbName(table.getCatName(), table.getDbName(), conf),
                                    table.getTableName(), false);
                        }

                        startIndex = endIndex;
                    }
                }

                if (ms.dropDatabase(catName, name)) {
                    if (!transactionalListeners.isEmpty()) {
                        transactionalListenerResponses =
                                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                        EventType.DROP_DATABASE,
                                        new DropDatabaseEvent(db, true, this));
                    }

                    success = ms.commitTransaction();
                }
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                } else if (deleteData) {
                    // Delete the data in the partitions which have other locations
//                    deletePartitionData(partitionPaths);
                    // Delete the data in the tables which have other locations
                    for (Path tablePath : tablePaths) {
                        deleteTableData(tablePath, false, db);
                    }
                    // Delete the data in the database
                    try {
                        wh.deleteDir(new Path(db.getLocationUri()), true, db);
                    } catch (Exception e) {
                        LOG.error("Failed to delete database directory: " + db.getLocationUri() +
                                " " + e.getMessage());
                    }
                    // it is not a terrible thing even if the data is not deleted
                }

                if (!listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(listeners,
                            EventType.DROP_DATABASE,
                            new DropDatabaseEvent(db, success, this),
                            null,
                            transactionalListenerResponses, ms);
                }
            }
        }

        private boolean isSubdirectory(Path parent, Path other) {
            return other.toString().startsWith(parent.toString().endsWith("/") ? parent.toString() : parent.toString() + "/");
        }

        public void drop_database(String dbName, boolean deleteData, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException {
            this.startFunction("drop_database", ": " + dbName);
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            if ("hive".equalsIgnoreCase(parsedDbName[0]) && "default".equalsIgnoreCase(parsedDbName[1])) {
                this.endFunction("drop_database", false, (Exception)null);
                throw new MetaException("Can not drop default database in catalog hive");
            } else {
                boolean success = false;
                Object ex = null;

                try {
                    this.drop_database_core(this.getMS(), parsedDbName[0], parsedDbName[1], deleteData, cascade);
                    success = true;
                } catch (InvalidOperationException | MetaException | NoSuchObjectException var12) {
                    ex = var12;
                    throw var12;
                } catch (Exception var13) {
                    ex = var13;
                    throw newMetaException(var13);
                } finally {
                    this.endFunction("drop_database", success, (Exception)ex);
                }

            }
        }

        public List<String> get_databases(String pattern) throws MetaException {
            this.startFunction("get_databases", ": " + pattern);
            String[] parsedDbNamed = MetaStoreUtils.parseDbName(pattern, this.conf);
            List<String> ret = null;
            Exception ex = null;

            try {
                if (parsedDbNamed[1] == null) {
                    ret = this.getMS().getAllDatabases(parsedDbNamed[0]);
                } else {
                    ret = this.getMS().getDatabases(parsedDbNamed[0], parsedDbNamed[1]);
                }
            } catch (Exception var9) {
                ex = var9;
                if (var9 instanceof MetaException) {
                    throw (MetaException)var9;
                }

                throw newMetaException(var9);
            } finally {
                this.endFunction("get_databases", ret != null, ex);
            }

            return ret;
        }

        public List<String> get_all_databases() throws MetaException {
            return this.get_databases(MetaStoreUtils.prependCatalogToDbName((String)null, (String)null, this.conf));
        }

        private void create_type_core(RawStore ms, Type type) throws AlreadyExistsException, MetaException, InvalidObjectException {
            if (!MetaStoreUtils.validateName(type.getName(), (Configuration)null)) {
                throw new InvalidObjectException("Invalid type name");
            } else {
                boolean success = false;

                try {
                    ms.openTransaction();
                    if (this.is_type_exists(ms, type.getName())) {
                        throw new AlreadyExistsException("Type " + type.getName() + " already exists");
                    }

                    ms.createType(type);
                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                    }

                }

            }
        }

        public boolean create_type(Type type) throws AlreadyExistsException, MetaException, InvalidObjectException {
            this.startFunction("create_type", ": " + type.toString());
            boolean success = false;
            Exception ex = null;

            try {
                this.create_type_core(this.getMS(), type);
                success = true;
            } catch (Exception var8) {
                ex = var8;
                if (var8 instanceof MetaException) {
                    throw (MetaException)var8;
                }

                if (var8 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var8;
                }

                if (var8 instanceof AlreadyExistsException) {
                    throw (AlreadyExistsException)var8;
                }

                throw newMetaException(var8);
            } finally {
                this.endFunction("create_type", success, ex);
            }

            return success;
        }

        public Type get_type(String name) throws MetaException, NoSuchObjectException {
            this.startFunction("get_type", ": " + name);
            Type ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().getType(name);
                if (null == ret) {
                    throw new NoSuchObjectException("Type \"" + name + "\" not found.");
                }
            } catch (Exception var8) {
                ex = var8;
                this.throwMetaException(var8);
            } finally {
                this.endFunction("get_type", ret != null, ex);
            }

            return ret;
        }

        private boolean is_type_exists(RawStore ms, String typeName) throws MetaException {
            return ms.getType(typeName) != null;
        }

        public boolean drop_type(String name) throws MetaException, NoSuchObjectException {
            this.startFunction("drop_type", ": " + name);
            boolean success = false;
            Exception ex = null;

            try {
                success = this.getMS().dropType(name);
            } catch (Exception var8) {
                ex = var8;
                this.throwMetaException(var8);
            } finally {
                this.endFunction("drop_type", success, ex);
            }

            return success;
        }

        public Map<String, Type> get_type_all(String name) throws MetaException {
            this.startFunction("get_type_all", ": " + name);
            this.endFunction("get_type_all", false, (Exception)null);
            throw new MetaException("Not yet implemented");
        }

        private void create_table_core(RawStore ms, Table tbl, EnvironmentContext envContext) throws AlreadyExistsException, MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
            this.create_table_core(ms, tbl, envContext, (List)null, (List)null, (List)null, (List)null, (List)null, (List)null);
        }

        private void create_table_core(RawStore ms, Table tbl, EnvironmentContext envContext, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints) throws AlreadyExistsException, MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
            ColumnStatistics colStats = null;
            if (tbl.isSetColStats()) {
                colStats = tbl.getColStats();
                tbl.unsetColStats();
            }

            if (!MetaStoreUtils.validateName(tbl.getTableName(), this.conf)) {
                throw new InvalidObjectException(tbl.getTableName() + " is not a valid object name");
            } else {
                String validate = MetaStoreUtils.validateTblColumns(tbl.getSd().getCols());
                if (validate != null) {
                    throw new InvalidObjectException("Invalid column " + validate);
                } else {
                    if (tbl.getPartitionKeys() != null) {
                        validate = MetaStoreUtils.validateTblColumns(tbl.getPartitionKeys());
                        if (validate != null) {
                            throw new InvalidObjectException("Invalid partition column " + validate);
                        }
                    }

                    SkewedInfo skew = tbl.getSd().getSkewedInfo();
                    if (skew != null) {
                        validate = MetaStoreUtils.validateSkewedColNames(skew.getSkewedColNames());
                        if (validate != null) {
                            throw new InvalidObjectException("Invalid skew column " + validate);
                        }

                        validate = MetaStoreUtils.validateSkewedColNamesSubsetCol(skew.getSkewedColNames(), tbl.getSd().getCols());
                        if (validate != null) {
                            throw new InvalidObjectException("Invalid skew column " + validate);
                        }
                    }

                    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
                    Path tblPath = null;
                    boolean success = false;
                    boolean madeDir = false;
                    Database db = null;

                    long time;
                    try {
                        if (!tbl.isSetCatName()) {
                            tbl.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
                        }

                        this.firePreEvent(new PreCreateTableEvent(tbl, this));
                        ms.openTransaction();
                        db = ms.getDatabase(tbl.getCatName(), tbl.getDbName());
                        if (this.is_table_exists(ms, tbl.getCatName(), tbl.getDbName(), tbl.getTableName())) {
                            throw new AlreadyExistsException("Table " + Warehouse.getCatalogQualifiedTableName(tbl) + " already exists");
                        }

                        if (!TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
                            if (tbl.getSd().getLocation() != null && !tbl.getSd().getLocation().isEmpty()) {
                                if (!this.isExternal(tbl) && !MetaStoreUtils.isNonNativeTable(tbl)) {
                                    LOG.warn("Location: " + tbl.getSd().getLocation() + " specified for non-external table:" + tbl.getTableName());
                                }

                                tblPath = this.wh.getDnsPath(new Path(tbl.getSd().getLocation()));
                            } else {
                                tblPath = this.wh.getDefaultTablePath(db, tbl);
                            }

                            tbl.getSd().setLocation(tblPath.toString());
                        }

                        if (tblPath != null && !this.wh.isDir(tblPath)) {
                            if (!this.wh.mkdirs(tblPath)) {
                                throw new MetaException(tblPath + " is not a directory or unable to create one");
                            }

                            madeDir = true;
                        }

                        if (MetastoreConf.getBoolVar(this.conf, ConfVars.STATS_AUTO_GATHER) && !MetaStoreUtils.isView(tbl)) {
                            MetaStoreUtils.updateTableStatsSlow(db, tbl, this.wh, madeDir, false, envContext);
                        }

                        time = System.currentTimeMillis() / 1000L;
                        tbl.setCreateTime((int)time);
                        if (tbl.getParameters() == null || tbl.getParameters().get("transient_lastDdlTime") == null) {
                            tbl.putToParameters("transient_lastDdlTime", Long.toString(time));
                        }

                        if (primaryKeys == null && foreignKeys == null && uniqueConstraints == null && notNullConstraints == null && defaultConstraints == null && checkConstraints == null) {
                            ms.createTable(tbl);
                        } else {
                            Iterator var20;
                            if (primaryKeys != null && !primaryKeys.isEmpty() && !((SQLPrimaryKey)primaryKeys.get(0)).isSetCatName()) {
                                var20 = primaryKeys.iterator();

                                while(var20.hasNext()) {
                                    SQLPrimaryKey pkcol = (SQLPrimaryKey)var20.next();
                                    pkcol.setCatName(tbl.getCatName());
                                }
                            }

                            if (foreignKeys != null && !foreignKeys.isEmpty() && !((SQLForeignKey)foreignKeys.get(0)).isSetCatName()) {
                                var20 = foreignKeys.iterator();

                                while(var20.hasNext()) {
                                    SQLForeignKey fkcol = (SQLForeignKey)var20.next();
                                    fkcol.setCatName(tbl.getCatName());
                                }
                            }

                            if (uniqueConstraints != null && !uniqueConstraints.isEmpty() && !((SQLUniqueConstraint)uniqueConstraints.get(0)).isSetCatName()) {
                                var20 = uniqueConstraints.iterator();

                                while(var20.hasNext()) {
                                    SQLUniqueConstraint uccol = (SQLUniqueConstraint)var20.next();
                                    uccol.setCatName(tbl.getCatName());
                                }
                            }

                            if (notNullConstraints != null && !notNullConstraints.isEmpty() && !((SQLNotNullConstraint)notNullConstraints.get(0)).isSetCatName()) {
                                var20 = notNullConstraints.iterator();

                                while(var20.hasNext()) {
                                    SQLNotNullConstraint nncol = (SQLNotNullConstraint)var20.next();
                                    nncol.setCatName(tbl.getCatName());
                                }
                            }

                            if (defaultConstraints != null && !defaultConstraints.isEmpty() && !((SQLDefaultConstraint)defaultConstraints.get(0)).isSetCatName()) {
                                var20 = defaultConstraints.iterator();

                                while(var20.hasNext()) {
                                    SQLDefaultConstraint dccol = (SQLDefaultConstraint)var20.next();
                                    dccol.setCatName(tbl.getCatName());
                                }
                            }

                            if (checkConstraints != null && !checkConstraints.isEmpty() && !((SQLCheckConstraint)checkConstraints.get(0)).isSetCatName()) {
                                var20 = checkConstraints.iterator();

                                while(var20.hasNext()) {
                                    SQLCheckConstraint cccol = (SQLCheckConstraint)var20.next();
                                    cccol.setCatName(tbl.getCatName());
                                }
                            }

                            List<String> constraintNames = ms.createTableWithConstraints(tbl, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
                            int primaryKeySize = 0;
                            int foreignKeySize;
                            if (primaryKeys != null) {
                                primaryKeySize = primaryKeys.size();

                                for(foreignKeySize = 0; foreignKeySize < primaryKeys.size(); ++foreignKeySize) {
                                    if (((SQLPrimaryKey)primaryKeys.get(foreignKeySize)).getPk_name() == null) {
                                        ((SQLPrimaryKey)primaryKeys.get(foreignKeySize)).setPk_name((String)constraintNames.get(foreignKeySize));
                                    }

                                    if (!((SQLPrimaryKey)primaryKeys.get(foreignKeySize)).isSetCatName()) {
                                        ((SQLPrimaryKey)primaryKeys.get(foreignKeySize)).setCatName(tbl.getCatName());
                                    }
                                }
                            }

                            foreignKeySize = 0;
                            int uniqueConstraintSize;
                            if (foreignKeys != null) {
                                foreignKeySize = foreignKeys.size();

                                for(uniqueConstraintSize = 0; uniqueConstraintSize < foreignKeySize; ++uniqueConstraintSize) {
                                    if (((SQLForeignKey)foreignKeys.get(uniqueConstraintSize)).getFk_name() == null) {
                                        ((SQLForeignKey)foreignKeys.get(uniqueConstraintSize)).setFk_name((String)constraintNames.get(primaryKeySize + uniqueConstraintSize));
                                    }

                                    if (!((SQLForeignKey)foreignKeys.get(uniqueConstraintSize)).isSetCatName()) {
                                        ((SQLForeignKey)foreignKeys.get(uniqueConstraintSize)).setCatName(tbl.getCatName());
                                    }
                                }
                            }

                            uniqueConstraintSize = 0;
                            if (uniqueConstraints != null) {
                                uniqueConstraintSize = uniqueConstraints.size();

                                for(int i = 0; i < uniqueConstraintSize; ++i) {
                                    if (((SQLUniqueConstraint)uniqueConstraints.get(i)).getUk_name() == null) {
                                        ((SQLUniqueConstraint)uniqueConstraints.get(i)).setUk_name((String)constraintNames.get(primaryKeySize + foreignKeySize + i));
                                    }

                                    if (!((SQLUniqueConstraint)uniqueConstraints.get(i)).isSetCatName()) {
                                        ((SQLUniqueConstraint)uniqueConstraints.get(i)).setCatName(tbl.getCatName());
                                    }
                                }
                            }

                            int notNullConstraintSize = 0;
                            if (notNullConstraints != null) {
                                for(int i = 0; i < notNullConstraints.size(); ++i) {
                                    if (((SQLNotNullConstraint)notNullConstraints.get(i)).getNn_name() == null) {
                                        ((SQLNotNullConstraint)notNullConstraints.get(i)).setNn_name((String)constraintNames.get(primaryKeySize + foreignKeySize + uniqueConstraintSize + i));
                                    }

                                    if (!((SQLNotNullConstraint)notNullConstraints.get(i)).isSetCatName()) {
                                        ((SQLNotNullConstraint)notNullConstraints.get(i)).setCatName(tbl.getCatName());
                                    }
                                }
                            }

                            int defaultConstraintSize = 0;
                            int i;
                            if (defaultConstraints != null) {
                                for(i = 0; i < defaultConstraints.size(); ++i) {
                                    if (((SQLDefaultConstraint)defaultConstraints.get(i)).getDc_name() == null) {
                                        ((SQLDefaultConstraint)defaultConstraints.get(i)).setDc_name((String)constraintNames.get(primaryKeySize + foreignKeySize + uniqueConstraintSize + notNullConstraintSize + i));
                                    }

                                    if (!((SQLDefaultConstraint)defaultConstraints.get(i)).isSetCatName()) {
                                        ((SQLDefaultConstraint)defaultConstraints.get(i)).setCatName(tbl.getCatName());
                                    }
                                }
                            }

                            if (checkConstraints != null) {
                                for(i = 0; i < checkConstraints.size(); ++i) {
                                    if (((SQLCheckConstraint)checkConstraints.get(i)).getDc_name() == null) {
                                        ((SQLCheckConstraint)checkConstraints.get(i)).setDc_name((String)constraintNames.get(primaryKeySize + foreignKeySize + uniqueConstraintSize + defaultConstraintSize + notNullConstraintSize + i));
                                    }

                                    if (!((SQLCheckConstraint)checkConstraints.get(i)).isSetCatName()) {
                                        ((SQLCheckConstraint)checkConstraints.get(i)).setCatName(tbl.getCatName());
                                    }
                                }
                            }
                        }

                        if (!this.transactionalListeners.isEmpty()) {
                            transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.CREATE_TABLE, new CreateTableEvent(tbl, true, this), envContext);
                            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                                MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ADD_PRIMARYKEY, new AddPrimaryKeyEvent(primaryKeys, true, this), envContext);
                            }

                            if (foreignKeys != null && !foreignKeys.isEmpty()) {
                                MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ADD_FOREIGNKEY, new AddForeignKeyEvent(foreignKeys, true, this), envContext);
                            }

                            if (uniqueConstraints != null && !uniqueConstraints.isEmpty()) {
                                MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ADD_UNIQUECONSTRAINT, new AddUniqueConstraintEvent(uniqueConstraints, true, this), envContext);
                            }

                            if (notNullConstraints != null && !notNullConstraints.isEmpty()) {
                                MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ADD_NOTNULLCONSTRAINT, new AddNotNullConstraintEvent(notNullConstraints, true, this), envContext);
                            }
                        }

                        success = ms.commitTransaction();
                    } finally {
                        if (!success) {
                            ms.rollbackTransaction();
                            if (madeDir) {
                                this.wh.deleteDir(tblPath, true, db);
                            }
                        }

                        if (!this.listeners.isEmpty()) {
                            MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.CREATE_TABLE, new CreateTableEvent(tbl, success, this), envContext, transactionalListenerResponses, ms);
                            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_PRIMARYKEY, new AddPrimaryKeyEvent(primaryKeys, success, this), envContext);
                            }

                            if (foreignKeys != null && !foreignKeys.isEmpty()) {
                                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_FOREIGNKEY, new AddForeignKeyEvent(foreignKeys, success, this), envContext);
                            }

                            if (uniqueConstraints != null && !uniqueConstraints.isEmpty()) {
                                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_UNIQUECONSTRAINT, new AddUniqueConstraintEvent(uniqueConstraints, success, this), envContext);
                            }

                            if (notNullConstraints != null && !notNullConstraints.isEmpty()) {
                                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_NOTNULLCONSTRAINT, new AddNotNullConstraintEvent(notNullConstraints, success, this), envContext);
                            }
                        }

                    }

                    if (colStats != null) {
                        time = tbl.getWriteId();
                        String validWriteIds = null;
                        if (time > 0L) {
                            ValidWriteIdList validWriteIdList = new ValidReaderWriteIdList(tbl.getDbName() + "." + tbl.getTableName(), new long[0], new BitSet(), time);
                            validWriteIds = validWriteIdList.toString();
                        }

                        this.updateTableColumnStatsInternal(colStats, validWriteIds, tbl.getWriteId());
                    }

                }
            }
        }

        public void create_table(Table tbl) throws AlreadyExistsException, MetaException, InvalidObjectException, InvalidInputException {
            this.create_table_with_environment_context(tbl, (EnvironmentContext)null);
        }

        public void create_table_with_environment_context(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException, MetaException, InvalidObjectException, InvalidInputException {
            this.startFunction("create_table", ": " + tbl.toString());
            boolean success = false;
            Object ex = null;

            try {
                this.create_table_core(this.getMS(), tbl, envContext);
                success = true;
            } catch (NoSuchObjectException var10) {
                LOG.warn("create_table_with_environment_context got ", var10);
                ex = var10;
                throw new InvalidObjectException(var10.getMessage());
            } catch (Exception var11) {
                ex = var11;
                if (var11 instanceof MetaException) {
                    throw (MetaException)var11;
                }

                if (var11 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var11;
                }

                if (var11 instanceof AlreadyExistsException) {
                    throw (AlreadyExistsException)var11;
                }

                if (var11 instanceof InvalidInputException) {
                    throw (InvalidInputException)var11;
                }

                throw newMetaException(var11);
            } finally {
                this.endFunction("create_table", success, (Exception)ex, tbl.getTableName());
            }

        }

        public void create_table_with_constraints(Table tbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints) throws AlreadyExistsException, MetaException, InvalidObjectException, InvalidInputException {
            this.startFunction("create_table", ": " + tbl.toString());
            boolean success = false;
            Object ex = null;

            try {
                this.create_table_core(this.getMS(), tbl, (EnvironmentContext)null, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
                success = true;
            } catch (NoSuchObjectException var15) {
                ex = var15;
                throw new InvalidObjectException(var15.getMessage());
            } catch (Exception var16) {
                ex = var16;
                if (var16 instanceof MetaException) {
                    throw (MetaException)var16;
                }

                if (var16 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var16;
                }

                if (var16 instanceof AlreadyExistsException) {
                    throw (AlreadyExistsException)var16;
                }

                if (var16 instanceof InvalidInputException) {
                    throw (InvalidInputException)var16;
                }

                throw newMetaException(var16);
            } finally {
                this.endFunction("create_table", success, (Exception)ex, tbl.getTableName());
            }

        }

        public void drop_constraint(DropConstraintRequest req) throws MetaException, InvalidObjectException {
            String catName = req.isSetCatName() ? req.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String dbName = req.getDbname();
            String tableName = req.getTablename();
            String constraintName = req.getConstraintname();
            this.startFunction("drop_constraint", ": " + constraintName);
            boolean success = false;
            Exception ex = null;
            RawStore ms = this.getMS();
            boolean var19 = false;

            try {
                var19 = true;
                ms.openTransaction();
                ms.dropConstraint(catName, dbName, tableName, constraintName);
                if (this.transactionalListeners.size() > 0) {
                    DropConstraintEvent dropConstraintEvent = new DropConstraintEvent(catName, dbName, tableName, constraintName, true, this);
                    Iterator var10 = this.transactionalListeners.iterator();

                    while(var10.hasNext()) {
                        MetaStoreEventListener transactionalListener = (MetaStoreEventListener)var10.next();
                        transactionalListener.onDropConstraint(dropConstraintEvent);
                    }
                }

                success = ms.commitTransaction();
                var19 = false;
            } catch (NoSuchObjectException var20) {
                ex = var20;
                throw new InvalidObjectException(var20.getMessage());
            } catch (Exception var21) {
                ex = var21;
                if (var21 instanceof MetaException) {
                    throw (MetaException)var21;
                }

                throw newMetaException(var21);
            } finally {
                if (var19) {
                    if (!success) {
                        ms.rollbackTransaction();
                    } else {
                        Iterator var13 = this.listeners.iterator();

                        while(var13.hasNext()) {
                            MetaStoreEventListener listener = (MetaStoreEventListener)var13.next();
                            DropConstraintEvent dropConstraintEvent = new DropConstraintEvent(catName, dbName, tableName, constraintName, true, this);
                            listener.onDropConstraint(dropConstraintEvent);
                        }
                    }

                    this.endFunction("drop_constraint", success, (Exception)ex, constraintName);
                }
            }

            if (!success) {
                ms.rollbackTransaction();
            } else {
                Iterator var23 = this.listeners.iterator();

                while(var23.hasNext()) {
                    MetaStoreEventListener listener = (MetaStoreEventListener)var23.next();
                    DropConstraintEvent dropConstraintEvent = new DropConstraintEvent(catName, dbName, tableName, constraintName, true, this);
                    listener.onDropConstraint(dropConstraintEvent);
                }
            }

            this.endFunction("drop_constraint", success, (Exception)ex, constraintName);
        }

        public void add_primary_key(AddPrimaryKeyRequest req) throws MetaException, InvalidObjectException {
            List<SQLPrimaryKey> primaryKeyCols = req.getPrimaryKeyCols();
            String constraintName = primaryKeyCols != null && primaryKeyCols.size() > 0 ? ((SQLPrimaryKey)primaryKeyCols.get(0)).getPk_name() : "null";
            this.startFunction("add_primary_key", ": " + constraintName);
            boolean success = false;
            Exception ex = null;
            if (!primaryKeyCols.isEmpty() && !((SQLPrimaryKey)primaryKeyCols.get(0)).isSetCatName()) {
                String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
                primaryKeyCols.forEach((pk) -> {
                    pk.setCatName(defaultCat);
                });
            }

            RawStore ms = this.getMS();
            boolean var17 = false;

            try {
                var17 = true;
                ms.openTransaction();
                List constraintNames = ms.addPrimaryKeys(primaryKeyCols);
                if (primaryKeyCols != null) {
                    for(int i = 0; i < primaryKeyCols.size(); ++i) {
                        if (((SQLPrimaryKey)primaryKeyCols.get(i)).getPk_name() == null) {
                            ((SQLPrimaryKey)primaryKeyCols.get(i)).setPk_name((String)constraintNames.get(i));
                        }
                    }
                }

                if (this.transactionalListeners.size() > 0 && primaryKeyCols != null && primaryKeyCols.size() > 0) {
                    AddPrimaryKeyEvent addPrimaryKeyEvent = new AddPrimaryKeyEvent(primaryKeyCols, true, this);
                    Iterator var9 = this.transactionalListeners.iterator();

                    while(var9.hasNext()) {
                        MetaStoreEventListener transactionalListener = (MetaStoreEventListener)var9.next();
                        transactionalListener.onAddPrimaryKey(addPrimaryKeyEvent);
                    }
                }

                success = ms.commitTransaction();
                var17 = false;
            } catch (Exception var18) {
                ex = var18;
                if (var18 instanceof MetaException) {
                    throw (MetaException)var18;
                }

                if (var18 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var18;
                }

                throw newMetaException(var18);
            } finally {
                if (var17) {
                    if (!success) {
                        ms.rollbackTransaction();
                    } else if (primaryKeyCols != null && primaryKeyCols.size() > 0) {
                        Iterator var12 = this.listeners.iterator();

                        while(var12.hasNext()) {
                            MetaStoreEventListener listener = (MetaStoreEventListener)var12.next();
                            AddPrimaryKeyEvent addPrimaryKeyEvent = new AddPrimaryKeyEvent(primaryKeyCols, true, this);
                            listener.onAddPrimaryKey(addPrimaryKeyEvent);
                        }
                    }

                    this.endFunction("add_primary_key", success, ex, constraintName);
                }
            }

            if (!success) {
                ms.rollbackTransaction();
            } else if (primaryKeyCols != null && primaryKeyCols.size() > 0) {
                Iterator var21 = this.listeners.iterator();

                while(var21.hasNext()) {
                    MetaStoreEventListener listener = (MetaStoreEventListener)var21.next();
                    AddPrimaryKeyEvent addPrimaryKeyEvent = new AddPrimaryKeyEvent(primaryKeyCols, true, this);
                    listener.onAddPrimaryKey(addPrimaryKeyEvent);
                }
            }

            this.endFunction("add_primary_key", success, ex, constraintName);
        }

        public void add_foreign_key(AddForeignKeyRequest req) throws MetaException, InvalidObjectException {
            List<SQLForeignKey> foreignKeyCols = req.getForeignKeyCols();
            String constraintName = foreignKeyCols != null && foreignKeyCols.size() > 0 ? ((SQLForeignKey)foreignKeyCols.get(0)).getFk_name() : "null";
            this.startFunction("add_foreign_key", ": " + constraintName);
            boolean success = false;
            Exception ex = null;
            if (!foreignKeyCols.isEmpty() && !((SQLForeignKey)foreignKeyCols.get(0)).isSetCatName()) {
                String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
                foreignKeyCols.forEach((pk) -> {
                    pk.setCatName(defaultCat);
                });
            }

            RawStore ms = this.getMS();
            boolean var17 = false;

            try {
                var17 = true;
                ms.openTransaction();
                List constraintNames = ms.addForeignKeys(foreignKeyCols);
                if (foreignKeyCols != null) {
                    for(int i = 0; i < foreignKeyCols.size(); ++i) {
                        if (((SQLForeignKey)foreignKeyCols.get(i)).getFk_name() == null) {
                            ((SQLForeignKey)foreignKeyCols.get(i)).setFk_name((String)constraintNames.get(i));
                        }
                    }
                }

                if (this.transactionalListeners.size() > 0 && foreignKeyCols != null && foreignKeyCols.size() > 0) {
                    AddForeignKeyEvent addForeignKeyEvent = new AddForeignKeyEvent(foreignKeyCols, true, this);
                    Iterator var9 = this.transactionalListeners.iterator();

                    while(var9.hasNext()) {
                        MetaStoreEventListener transactionalListener = (MetaStoreEventListener)var9.next();
                        transactionalListener.onAddForeignKey(addForeignKeyEvent);
                    }
                }

                success = ms.commitTransaction();
                var17 = false;
            } catch (Exception var18) {
                ex = var18;
                if (var18 instanceof MetaException) {
                    throw (MetaException)var18;
                }

                if (var18 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var18;
                }

                throw newMetaException(var18);
            } finally {
                if (var17) {
                    if (!success) {
                        ms.rollbackTransaction();
                    } else if (foreignKeyCols != null && foreignKeyCols.size() > 0) {
                        Iterator var12 = this.listeners.iterator();

                        while(var12.hasNext()) {
                            MetaStoreEventListener listener = (MetaStoreEventListener)var12.next();
                            AddForeignKeyEvent addForeignKeyEvent = new AddForeignKeyEvent(foreignKeyCols, true, this);
                            listener.onAddForeignKey(addForeignKeyEvent);
                        }
                    }

                    this.endFunction("add_foreign_key", success, ex, constraintName);
                }
            }

            if (!success) {
                ms.rollbackTransaction();
            } else if (foreignKeyCols != null && foreignKeyCols.size() > 0) {
                Iterator var21 = this.listeners.iterator();

                while(var21.hasNext()) {
                    MetaStoreEventListener listener = (MetaStoreEventListener)var21.next();
                    AddForeignKeyEvent addForeignKeyEvent = new AddForeignKeyEvent(foreignKeyCols, true, this);
                    listener.onAddForeignKey(addForeignKeyEvent);
                }
            }

            this.endFunction("add_foreign_key", success, ex, constraintName);
        }

        public void add_unique_constraint(AddUniqueConstraintRequest req) throws MetaException, InvalidObjectException {
            List<SQLUniqueConstraint> uniqueConstraintCols = req.getUniqueConstraintCols();
            String constraintName = uniqueConstraintCols != null && uniqueConstraintCols.size() > 0 ? ((SQLUniqueConstraint)uniqueConstraintCols.get(0)).getUk_name() : "null";
            this.startFunction("add_unique_constraint", ": " + constraintName);
            boolean success = false;
            Exception ex = null;
            if (!uniqueConstraintCols.isEmpty() && !((SQLUniqueConstraint)uniqueConstraintCols.get(0)).isSetCatName()) {
                String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
                uniqueConstraintCols.forEach((pk) -> {
                    pk.setCatName(defaultCat);
                });
            }

            RawStore ms = this.getMS();
            boolean var17 = false;

            try {
                var17 = true;
                ms.openTransaction();
                List constraintNames = ms.addUniqueConstraints(uniqueConstraintCols);
                if (uniqueConstraintCols != null) {
                    for(int i = 0; i < uniqueConstraintCols.size(); ++i) {
                        if (((SQLUniqueConstraint)uniqueConstraintCols.get(i)).getUk_name() == null) {
                            ((SQLUniqueConstraint)uniqueConstraintCols.get(i)).setUk_name((String)constraintNames.get(i));
                        }
                    }
                }

                if (this.transactionalListeners.size() > 0 && uniqueConstraintCols != null && uniqueConstraintCols.size() > 0) {
                    AddUniqueConstraintEvent addUniqueConstraintEvent = new AddUniqueConstraintEvent(uniqueConstraintCols, true, this);
                    Iterator var9 = this.transactionalListeners.iterator();

                    while(var9.hasNext()) {
                        MetaStoreEventListener transactionalListener = (MetaStoreEventListener)var9.next();
                        transactionalListener.onAddUniqueConstraint(addUniqueConstraintEvent);
                    }
                }

                success = ms.commitTransaction();
                var17 = false;
            } catch (Exception var18) {
                ex = var18;
                if (var18 instanceof MetaException) {
                    throw (MetaException)var18;
                }

                if (var18 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var18;
                }

                throw newMetaException(var18);
            } finally {
                if (var17) {
                    if (!success) {
                        ms.rollbackTransaction();
                    } else if (uniqueConstraintCols != null && uniqueConstraintCols.size() > 0) {
                        Iterator var12 = this.listeners.iterator();

                        while(var12.hasNext()) {
                            MetaStoreEventListener listener = (MetaStoreEventListener)var12.next();
                            AddUniqueConstraintEvent addUniqueConstraintEvent = new AddUniqueConstraintEvent(uniqueConstraintCols, true, this);
                            listener.onAddUniqueConstraint(addUniqueConstraintEvent);
                        }
                    }

                    this.endFunction("add_unique_constraint", success, ex, constraintName);
                }
            }

            if (!success) {
                ms.rollbackTransaction();
            } else if (uniqueConstraintCols != null && uniqueConstraintCols.size() > 0) {
                Iterator var21 = this.listeners.iterator();

                while(var21.hasNext()) {
                    MetaStoreEventListener listener = (MetaStoreEventListener)var21.next();
                    AddUniqueConstraintEvent addUniqueConstraintEvent = new AddUniqueConstraintEvent(uniqueConstraintCols, true, this);
                    listener.onAddUniqueConstraint(addUniqueConstraintEvent);
                }
            }

            this.endFunction("add_unique_constraint", success, ex, constraintName);
        }

        public void add_not_null_constraint(AddNotNullConstraintRequest req) throws MetaException, InvalidObjectException {
            List<SQLNotNullConstraint> notNullConstraintCols = req.getNotNullConstraintCols();
            String constraintName = notNullConstraintCols != null && notNullConstraintCols.size() > 0 ? ((SQLNotNullConstraint)notNullConstraintCols.get(0)).getNn_name() : "null";
            this.startFunction("add_not_null_constraint", ": " + constraintName);
            boolean success = false;
            Exception ex = null;
            if (!notNullConstraintCols.isEmpty() && !((SQLNotNullConstraint)notNullConstraintCols.get(0)).isSetCatName()) {
                String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
                notNullConstraintCols.forEach((pk) -> {
                    pk.setCatName(defaultCat);
                });
            }

            RawStore ms = this.getMS();
            boolean var17 = false;

            try {
                var17 = true;
                ms.openTransaction();
                List constraintNames = ms.addNotNullConstraints(notNullConstraintCols);
                if (notNullConstraintCols != null) {
                    for(int i = 0; i < notNullConstraintCols.size(); ++i) {
                        if (((SQLNotNullConstraint)notNullConstraintCols.get(i)).getNn_name() == null) {
                            ((SQLNotNullConstraint)notNullConstraintCols.get(i)).setNn_name((String)constraintNames.get(i));
                        }
                    }
                }

                if (this.transactionalListeners.size() > 0 && notNullConstraintCols != null && notNullConstraintCols.size() > 0) {
                    AddNotNullConstraintEvent addNotNullConstraintEvent = new AddNotNullConstraintEvent(notNullConstraintCols, true, this);
                    Iterator var9 = this.transactionalListeners.iterator();

                    while(var9.hasNext()) {
                        MetaStoreEventListener transactionalListener = (MetaStoreEventListener)var9.next();
                        transactionalListener.onAddNotNullConstraint(addNotNullConstraintEvent);
                    }
                }

                success = ms.commitTransaction();
                var17 = false;
            } catch (Exception var18) {
                ex = var18;
                if (var18 instanceof MetaException) {
                    throw (MetaException)var18;
                }

                if (var18 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var18;
                }

                throw newMetaException(var18);
            } finally {
                if (var17) {
                    if (!success) {
                        ms.rollbackTransaction();
                    } else if (notNullConstraintCols != null && notNullConstraintCols.size() > 0) {
                        Iterator var12 = this.listeners.iterator();

                        while(var12.hasNext()) {
                            MetaStoreEventListener listener = (MetaStoreEventListener)var12.next();
                            AddNotNullConstraintEvent addNotNullConstraintEvent = new AddNotNullConstraintEvent(notNullConstraintCols, true, this);
                            listener.onAddNotNullConstraint(addNotNullConstraintEvent);
                        }
                    }

                    this.endFunction("add_not_null_constraint", success, ex, constraintName);
                }
            }

            if (!success) {
                ms.rollbackTransaction();
            } else if (notNullConstraintCols != null && notNullConstraintCols.size() > 0) {
                Iterator var21 = this.listeners.iterator();

                while(var21.hasNext()) {
                    MetaStoreEventListener listener = (MetaStoreEventListener)var21.next();
                    AddNotNullConstraintEvent addNotNullConstraintEvent = new AddNotNullConstraintEvent(notNullConstraintCols, true, this);
                    listener.onAddNotNullConstraint(addNotNullConstraintEvent);
                }
            }

            this.endFunction("add_not_null_constraint", success, ex, constraintName);
        }

        public void add_default_constraint(AddDefaultConstraintRequest req) throws MetaException, InvalidObjectException {
            List<SQLDefaultConstraint> defaultConstraintCols = req.getDefaultConstraintCols();
            String constraintName = defaultConstraintCols != null && defaultConstraintCols.size() > 0 ? ((SQLDefaultConstraint)defaultConstraintCols.get(0)).getDc_name() : "null";
            this.startFunction("add_default_constraint", ": " + constraintName);
            boolean success = false;
            Exception ex = null;
            if (!defaultConstraintCols.isEmpty() && !((SQLDefaultConstraint)defaultConstraintCols.get(0)).isSetCatName()) {
                String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
                defaultConstraintCols.forEach((pk) -> {
                    pk.setCatName(defaultCat);
                });
            }

            RawStore ms = this.getMS();
            boolean var14 = false;

            try {
                var14 = true;
                ms.openTransaction();
                List constraintNames = ms.addDefaultConstraints(defaultConstraintCols);
                if (defaultConstraintCols != null) {
                    for(int i = 0; i < defaultConstraintCols.size(); ++i) {
                        if (((SQLDefaultConstraint)defaultConstraintCols.get(i)).getDc_name() == null) {
                            ((SQLDefaultConstraint)defaultConstraintCols.get(i)).setDc_name((String)constraintNames.get(i));
                        }
                    }
                }

                if (this.transactionalListeners.size() > 0 && defaultConstraintCols != null && defaultConstraintCols.size() > 0) {
                }

                success = ms.commitTransaction();
                var14 = false;
            } catch (Exception var15) {
                ex = var15;
                if (var15 instanceof MetaException) {
                    throw (MetaException)var15;
                }

                if (var15 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var15;
                }

                throw newMetaException(var15);
            } finally {
                if (var14) {
                    if (!success) {
                        ms.rollbackTransaction();
                    } else {
                        MetaStoreEventListener var11;
                        if (defaultConstraintCols != null && defaultConstraintCols.size() > 0) {
                            for(Iterator var10 = this.listeners.iterator(); var10.hasNext(); var11 = (MetaStoreEventListener)var10.next()) {
                            }
                        }
                    }

                    this.endFunction("add_default_constraint", success, ex, constraintName);
                }
            }

            if (!success) {
                ms.rollbackTransaction();
            } else {
                MetaStoreEventListener var19;
                if (defaultConstraintCols != null && defaultConstraintCols.size() > 0) {
                    for(Iterator var18 = this.listeners.iterator(); var18.hasNext(); var19 = (MetaStoreEventListener)var18.next()) {
                    }
                }
            }

            this.endFunction("add_default_constraint", success, ex, constraintName);
        }

        public void add_check_constraint(AddCheckConstraintRequest req) throws MetaException, InvalidObjectException {
            List<SQLCheckConstraint> checkConstraintCols = req.getCheckConstraintCols();
            String constraintName = checkConstraintCols != null && checkConstraintCols.size() > 0 ? ((SQLCheckConstraint)checkConstraintCols.get(0)).getDc_name() : "null";
            this.startFunction("add_check_constraint", ": " + constraintName);
            boolean success = false;
            Exception ex = null;
            if (!checkConstraintCols.isEmpty() && !((SQLCheckConstraint)checkConstraintCols.get(0)).isSetCatName()) {
                String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
                checkConstraintCols.forEach((pk) -> {
                    pk.setCatName(defaultCat);
                });
            }

            RawStore ms = this.getMS();
            boolean var14 = false;

            try {
                var14 = true;
                ms.openTransaction();
                List constraintNames = ms.addCheckConstraints(checkConstraintCols);
                if (checkConstraintCols != null) {
                    for(int i = 0; i < checkConstraintCols.size(); ++i) {
                        if (((SQLCheckConstraint)checkConstraintCols.get(i)).getDc_name() == null) {
                            ((SQLCheckConstraint)checkConstraintCols.get(i)).setDc_name((String)constraintNames.get(i));
                        }
                    }
                }

                if (this.transactionalListeners.size() > 0 && checkConstraintCols != null && checkConstraintCols.size() > 0) {
                }

                success = ms.commitTransaction();
                var14 = false;
            } catch (Exception var15) {
                ex = var15;
                if (var15 instanceof MetaException) {
                    throw (MetaException)var15;
                }

                if (var15 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var15;
                }

                throw newMetaException(var15);
            } finally {
                if (var14) {
                    if (!success) {
                        ms.rollbackTransaction();
                    } else {
                        MetaStoreEventListener var11;
                        if (checkConstraintCols != null && checkConstraintCols.size() > 0) {
                            for(Iterator var10 = this.listeners.iterator(); var10.hasNext(); var11 = (MetaStoreEventListener)var10.next()) {
                            }
                        }
                    }

                    this.endFunction("add_check_constraint", success, ex, constraintName);
                }
            }

            if (!success) {
                ms.rollbackTransaction();
            } else {
                MetaStoreEventListener var19;
                if (checkConstraintCols != null && checkConstraintCols.size() > 0) {
                    for(Iterator var18 = this.listeners.iterator(); var18.hasNext(); var19 = (MetaStoreEventListener)var18.next()) {
                    }
                }
            }

            this.endFunction("add_check_constraint", success, ex, constraintName);
        }

        private boolean is_table_exists(RawStore ms, String catName, String dbname, String name) throws MetaException {
            return ms.getTable(catName, dbname, name, (String)null) != null;
        }

        private boolean drop_table_core(RawStore ms, String catName, String dbname, String name, boolean deleteData, EnvironmentContext envContext, String indexName) throws NoSuchObjectException, MetaException, IOException, InvalidObjectException, InvalidInputException {
            boolean success = false;
            boolean tableDataShouldBeDeleted = false;
            Path tblPath = null;
            List<Path> partPaths = null;
            Table tbl = null;
            boolean ifPurge = false;
            Map<String, String> transactionalListenerResponses = Collections.emptyMap();
            Database db = null;

            try {
                ms.openTransaction();
                db = ms.getDatabase(catName, dbname);
                tbl = this.get_table_core(catName, dbname, name);
                if (tbl == null) {
                    throw new NoSuchObjectException(name + " doesn't exist");
                }

                if (tbl.getSd() == null) {
                    throw new MetaException("Table metadata is corrupted");
                }

                ifPurge = isMustPurge(envContext, tbl);
                this.firePreEvent(new PreDropTableEvent(tbl, deleteData, this));
                tableDataShouldBeDeleted = this.checkTableDataShouldBeDeleted(tbl, deleteData);
                String tableName;
                if (tbl.getSd().getLocation() != null) {
                    tblPath = new Path(tbl.getSd().getLocation());
                    if (!this.wh.isWritable(tblPath.getParent())) {
                        tableName = indexName == null ? "Table" : "Index table";
                        throw new MetaException(tableName + " metadata not deleted since " + tblPath.getParent() + " is not writable by " + SecurityUtils.getUser());
                    }
                }

                partPaths = this.dropPartitionsAndGetLocations(ms, catName, dbname, name, tblPath, tbl.getPartitionKeys(), tableDataShouldBeDeleted);
                ms.dropConstraint(catName, dbname, name, (String)null, true);
                if (!ms.dropTable(catName, dbname, name)) {
                    tableName = Warehouse.getCatalogQualifiedTableName(catName, dbname, name);
                    throw new MetaException(indexName == null ? "Unable to drop table " + tableName : "Unable to drop index table " + tableName + " for index " + indexName);
                }

                if (!this.transactionalListeners.isEmpty()) {
                    transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.DROP_TABLE, new DropTableEvent(tbl, true, deleteData, this), envContext);
                }

                success = ms.commitTransaction();
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                } else if (tableDataShouldBeDeleted) {
                    this.deletePartitionData(partPaths, ifPurge, db);
                    this.deleteTableData(tblPath, ifPurge, db);
                }

                if (!this.listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.DROP_TABLE, new DropTableEvent(tbl, success, deleteData, this), envContext, transactionalListenerResponses, ms);
                }

            }

            return success;
        }

        private boolean checkTableDataShouldBeDeleted(Table tbl, boolean deleteData) {
            return deleteData && this.isExternal(tbl) ? this.isExternalTablePurge(tbl) : deleteData;
        }

        private void deleteTableData(Path tablePath, boolean ifPurge, Database db) {
            if (tablePath != null) {
                try {
                    this.wh.deleteDir(tablePath, true, ifPurge, db);
                } catch (Exception var5) {
                    LOG.error("Failed to delete table directory: " + tablePath + " " + var5.getMessage());
                }
            }

        }

        private void deletePartitionData(List<Path> partPaths, boolean ifPurge, Database db) {
            if (partPaths != null && !partPaths.isEmpty()) {
                Iterator var4 = partPaths.iterator();

                while(var4.hasNext()) {
                    Path partPath = (Path)var4.next();

                    try {
                        this.wh.deleteDir(partPath, true, ifPurge, db);
                    } catch (Exception var7) {
                        LOG.error("Failed to delete partition directory: " + partPath + " " + var7.getMessage());
                    }
                }
            }

        }

        private List<Path> dropPartitionsAndGetLocations(RawStore ms, String catName, String dbName, String tableName, Path tablePath, List<FieldSchema> partitionKeys, boolean checkLocation) throws MetaException, IOException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
            int partitionBatchSize = MetastoreConf.getIntVar(conf,
                    ConfVars.BATCH_RETRIEVE_MAX);
            Path tableDnsPath = null;
            if (tablePath != null) {
                tableDnsPath = wh.getDnsPath(tablePath);
            }
            List<Path> partPaths = new ArrayList<>();
            Table tbl = ms.getTable(catName, dbName, tableName);

            // call dropPartition on each of the table's partitions to follow the
            // procedure for cleanly dropping partitions.
            while (true) {
                List<Partition> partsToDelete = ms.getPartitions(catName, dbName, tableName, partitionBatchSize);
                if (partsToDelete == null || partsToDelete.isEmpty()) {
                    break;
                }
                List<String> partNames = new ArrayList<>();
                for (Partition part : partsToDelete) {
                    if (checkLocation && part.getSd() != null &&
                            part.getSd().getLocation() != null) {

                        Path partPath = wh.getDnsPath(new Path(part.getSd().getLocation()));
                        if (tableDnsPath == null ||
                                (partPath != null && !isSubdirectory(tableDnsPath, partPath))) {
                            if (!wh.isWritable(partPath.getParent())) {
                                throw new MetaException("Table metadata not deleted since the partition " +
                                        Warehouse.makePartName(partitionKeys, part.getValues()) +
                                        " has parent location " + partPath.getParent() + " which is not writable " +
                                        "by " + SecurityUtils.getUser());
                            }
                            partPaths.add(partPath);
                        }
                    }
                    partNames.add(Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues()));
                }
                for (MetaStoreEventListener listener : listeners) {
                    //No drop part listener events fired for public listeners historically, for drop table case.
                    //Limiting to internal listeners for now, to avoid unexpected calls for public listeners.
                    if (listener instanceof HMSMetricsListener) {
                        for (@SuppressWarnings("unused") Partition part : partsToDelete) {
                            listener.onDropPartition(null);
                        }
                    }
                }
                ms.dropPartitions(catName, dbName, tableName, partNames);
            }

            return partPaths;
        }

        public void drop_table(String dbname, String name, boolean deleteData) throws NoSuchObjectException, MetaException {
            this.drop_table_with_environment_context(dbname, name, deleteData, (EnvironmentContext)null);
        }

        public void drop_table_with_environment_context(String dbname, String name, boolean deleteData, EnvironmentContext envContext) throws NoSuchObjectException, MetaException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbname, this.conf);
            this.startTableFunction("drop_table", parsedDbName[0], parsedDbName[1], name);
            boolean success = false;
            Object ex = null;

            try {
                success = this.drop_table_core(this.getMS(), parsedDbName[0], parsedDbName[1], name, deleteData, envContext, (String)null);
            } catch (IOException var13) {
                ex = var13;
                throw new MetaException(var13.getMessage());
            } catch (Exception var14) {
                ex = var14;
                this.throwMetaException(var14);
            } finally {
                this.endFunction("drop_table", success, (Exception)ex, name);
            }

        }

        private void updateStatsForTruncate(Map<String, String> props, EnvironmentContext environmentContext) {
            if (null != props) {
                String[] var3 = StatsSetupConst.supportedStats;
                int var4 = var3.length;

                for(int var5 = 0; var5 < var4; ++var5) {
                    String stat = var3[var5];
                    String statVal = (String)props.get(stat);
                    if (statVal != null) {
                        props.put(stat, "0");
                    }
                }

                StatsSetupConst.setBasicStatsState(props, "true");
                environmentContext.putToProperties("STATS_GENERATED", "TASK");
                StatsSetupConst.clearColumnStatsState(props);
            }
        }

        private void alterPartitionForTruncate(RawStore ms, String catName, String dbName, String tableName, Table table, Partition partition, String validWriteIds, long writeId) throws Exception {
            EnvironmentContext environmentContext = new EnvironmentContext();
            this.updateStatsForTruncate(partition.getParameters(), environmentContext);
            if (!this.transactionalListeners.isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ALTER_PARTITION, new AlterPartitionEvent(partition, partition, table, true, true, writeId, this));
            }

            if (!this.listeners.isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALTER_PARTITION, new AlterPartitionEvent(partition, partition, table, true, true, writeId, this));
            }

            if (writeId > 0L) {
                partition.setWriteId(writeId);
            }

            this.alterHandler.alterPartition(ms, this.wh, catName, dbName, tableName, (List)null, partition, environmentContext, this, validWriteIds);
        }

        private void alterTableStatsForTruncate(RawStore ms, String catName, String dbName, String tableName, Table table, List<String> partNames, String validWriteIds, long writeId) throws Exception {
            Iterator var10;
            Partition partition;
            if (partNames == null) {
                if (0 != table.getPartitionKeysSize()) {
                    var10 = ms.getPartitions(catName, dbName, tableName, 2147483647).iterator();

                    while(var10.hasNext()) {
                        partition = (Partition)var10.next();
                        this.alterPartitionForTruncate(ms, catName, dbName, tableName, table, partition, validWriteIds, writeId);
                    }
                } else {
                    EnvironmentContext environmentContext = new EnvironmentContext();
                    this.updateStatsForTruncate(table.getParameters(), environmentContext);
                    if (!this.transactionalListeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ALTER_TABLE, new AlterTableEvent(table, table, true, true, writeId, this));
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALTER_TABLE, new AlterTableEvent(table, table, true, true, writeId, this));
                    }

                    if (writeId > 0L) {
                        table.setWriteId(writeId);
                    }

                    this.alterHandler.alterTable(ms, this.wh, catName, dbName, tableName, table, environmentContext, this, validWriteIds);
                }
            } else {
                var10 = ms.getPartitionsByNames(catName, dbName, tableName, partNames).iterator();

                while(var10.hasNext()) {
                    partition = (Partition)var10.next();
                    this.alterPartitionForTruncate(ms, catName, dbName, tableName, table, partition, validWriteIds, writeId);
                }
            }

        }

        private List<Path> getLocationsForTruncate(RawStore ms, String catName, String dbName, String tableName, Table table, List<String> partNames) throws Exception {
            List<Path> locations = new ArrayList();
            Iterator var8;
            Partition partition;
            if (partNames == null) {
                if (0 != table.getPartitionKeysSize()) {
                    var8 = ms.getPartitions(catName, dbName, tableName, 2147483647).iterator();

                    while(var8.hasNext()) {
                        partition = (Partition)var8.next();
                        locations.add(new Path(partition.getSd().getLocation()));
                    }
                } else {
                    locations.add(new Path(table.getSd().getLocation()));
                }
            } else {
                var8 = ms.getPartitionsByNames(catName, dbName, tableName, partNames).iterator();

                while(var8.hasNext()) {
                    partition = (Partition)var8.next();
                    locations.add(new Path(partition.getSd().getLocation()));
                }
            }

            return locations;
        }

        public CmRecycleResponse cm_recycle(CmRecycleRequest request) throws MetaException {
            this.wh.recycleDirToCmPath(new Path(request.getDataPath()), request.isPurge());
            return new CmRecycleResponse();
        }

        public void truncate_table(String dbName, String tableName, List<String> partNames) throws NoSuchObjectException, MetaException {
            this.truncateTableInternal(dbName, tableName, partNames, (String)null, -1L);
        }

        public TruncateTableResponse truncate_table_req(TruncateTableRequest req) throws MetaException, TException {
            this.truncateTableInternal(req.getDbName(), req.getTableName(), req.getPartNames(), req.getValidWriteIdList(), req.getWriteId());
            return new TruncateTableResponse();
        }

        private void truncateTableInternal(String dbName, String tableName, List<String> partNames, String validWriteIds, long writeId) throws MetaException, NoSuchObjectException {
            try {
                String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
                Table tbl = this.get_table_core(parsedDbName[0], parsedDbName[1], tableName);
                boolean isAutopurge = tbl.isSetParameters() && "true".equalsIgnoreCase((String)tbl.getParameters().get("auto.purge"));
                Database db = this.get_database_core(parsedDbName[0], parsedDbName[1]);
                Iterator var11 = this.getLocationsForTruncate(this.getMS(), parsedDbName[0], parsedDbName[1], tableName, tbl, partNames).iterator();

                while(true) {
                    while(var11.hasNext()) {
                        Path location = (Path)var11.next();
                        FileSystem fs = location.getFileSystem(this.getConf());
                        if (!HdfsUtils.isPathEncrypted(this.getConf(), fs.getUri(), location) && !FileUtils.pathHasSnapshotSubDir(location, fs)) {
                            HadoopFileStatus status = new HadoopFileStatus(this.getConf(), fs, location);
                            FileStatus targetStatus = fs.getFileStatus(location);
                            String targetGroup = targetStatus == null ? null : targetStatus.getGroup();
                            this.wh.deleteDir(location, true, isAutopurge, db);
                            fs.mkdirs(location);
                            HdfsUtils.setFullFileStatus(this.getConf(), status, targetGroup, fs, location, false);
                        } else {
                            FileStatus[] statuses = fs.listStatus(location, FileUtils.HIDDEN_FILES_PATH_FILTER);
                            if (statuses != null && statuses.length != 0) {
                                FileStatus[] var15 = statuses;
                                int var16 = statuses.length;

                                for(int var17 = 0; var17 < var16; ++var17) {
                                    FileStatus status = var15[var17];
                                    this.wh.deleteDir(status.getPath(), true, isAutopurge, db);
                                }
                            }
                        }
                    }

                    this.alterTableStatsForTruncate(this.getMS(), parsedDbName[0], parsedDbName[1], tableName, tbl, partNames, validWriteIds, writeId);
                    return;
                }
            } catch (IOException var19) {
                throw new MetaException(var19.getMessage());
            } catch (Exception var20) {
                if (var20 instanceof MetaException) {
                    throw (MetaException)var20;
                } else if (var20 instanceof NoSuchObjectException) {
                    throw (NoSuchObjectException)var20;
                } else {
                    throw newMetaException(var20);
                }
            }
        }

        private boolean isExternal(Table table) {
            return MetaStoreUtils.isExternalTable(table);
        }

        private boolean isExternalTablePurge(Table table) {
            return MetaStoreUtils.isPropertyTrue(table.getParameters(), "external.table.purge");
        }

        /** @deprecated */
        @Deprecated
        public Table get_table(String dbname, String name) throws MetaException, NoSuchObjectException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbname, this.conf);
            return this.getTableInternal(parsedDbName[0], parsedDbName[1], name, (ClientCapabilities)null, (String)null, false);
        }

        public GetTableResult get_table_req(GetTableRequest req) throws MetaException, NoSuchObjectException {
            String catName = req.isSetCatName() ? req.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            return new GetTableResult(this.getTableInternal(catName, req.getDbName(), req.getTblName(), req.getCapabilities(), req.getValidWriteIdList(), req.isGetColumnStats()));
        }

        private Table getTableInternal(String catName, String dbname, String name, ClientCapabilities capabilities, String writeIdList, boolean getColumnStats) throws MetaException, NoSuchObjectException {
            if (this.isInTest) {
                this.assertClientHasCapability(capabilities, ClientCapability.TEST_CAPABILITY, "Hive tests", "get_table_req");
            }

            Table t = null;
            this.startTableFunction("get_table", catName, dbname, name);
            Exception ex = null;

            try {
                t = this.get_table_core(catName, dbname, name, writeIdList, getColumnStats);
                if (MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
                    this.assertClientHasCapability(capabilities, ClientCapability.INSERT_ONLY_TABLES, "insert-only tables", "get_table_req");
                }

                this.firePreEvent(new PreReadTableEvent(t, this));
            } catch (NoSuchObjectException | MetaException var13) {
                ex = var13;
                throw var13;
            } finally {
                this.endFunction("get_table", t != null, ex, name);
            }

            return t;
        }

        public List<TableMeta> get_table_meta(String dbnames, String tblNames, List<String> tblTypes) throws MetaException, NoSuchObjectException {
            List<TableMeta> t = null;
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbnames, this.conf);
            this.startTableFunction("get_table_metas", parsedDbName[0], parsedDbName[1], tblNames);
            Exception ex = null;

            try {
                t = this.getMS().getTableMeta(parsedDbName[0], parsedDbName[1], tblNames, tblTypes);
            } catch (Exception var11) {
                ex = var11;
                throw newMetaException(var11);
            } finally {
                this.endFunction("get_table_metas", t != null, ex);
            }

            return t;
        }

        public Table get_table_core(String catName, String dbname, String name) throws MetaException, NoSuchObjectException {
            return this.get_table_core(catName, dbname, name, (String)null);
        }

        public Table get_table_core(String catName, String dbname, String name, String writeIdList) throws MetaException, NoSuchObjectException {
            return this.get_table_core(catName, dbname, name, writeIdList, false);
        }

        public Table get_table_core(String catName, String dbname, String name, String writeIdList, boolean getColumnStats) throws MetaException, NoSuchObjectException {
            Table t = null;

            try {
                t = this.getMS().getTable(catName, dbname, name, writeIdList);
                if (t == null) {
                    throw new NoSuchObjectException(Warehouse.getCatalogQualifiedTableName(catName, dbname, name) + " table not found");
                }

                if (getColumnStats) {
                    ColumnStatistics colStats = this.getMS().getTableColumnStatistics(catName, dbname, name, StatsSetupConst.getColumnsHavingStats(t.getParameters()), writeIdList);
                    if (colStats != null) {
                        t.setColStats(colStats);
                    }
                }
            } catch (Exception var8) {
                this.throwMetaException(var8);
            }

            return t;
        }

        /** @deprecated */
        @Deprecated
        public List<Table> get_table_objects_by_name(String dbName, List<String> tableNames) throws MetaException, InvalidOperationException, UnknownDBException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            return this.getTableObjectsInternal(parsedDbName[0], parsedDbName[1], tableNames, (ClientCapabilities)null);
        }

        public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req) throws TException {
            String catName = req.isSetCatName() ? req.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            return new GetTablesResult(this.getTableObjectsInternal(catName, req.getDbName(), req.getTblNames(), req.getCapabilities()));
        }

        private List<Table> getTableObjectsInternal(String catName, String dbName, List<String> tableNames, ClientCapabilities capabilities) throws MetaException, InvalidOperationException, UnknownDBException {
            if (this.isInTest) {
                this.assertClientHasCapability(capabilities, ClientCapability.TEST_CAPABILITY, "Hive tests", "get_table_objects_by_name_req");
            }

            List<Table> tables = new ArrayList();
            this.startMultiTableFunction("get_multi_table", dbName, tableNames);
            Exception ex = null;
            int tableBatchSize = MetastoreConf.getIntVar(this.conf, ConfVars.BATCH_RETRIEVE_MAX);

            try {
                if (dbName == null || dbName.isEmpty()) {
                    throw new UnknownDBException("DB name is null or empty");
                } else if (tableNames == null) {
                    throw new InvalidOperationException(dbName + " cannot find null tables");
                } else {
                    List<String> distinctTableNames = tableNames;
                    if (tableNames.size() > tableBatchSize) {
                        List<String> lowercaseTableNames = new ArrayList();
                        Iterator var10 = tableNames.iterator();

                        while(true) {
                            if (!var10.hasNext()) {
                                distinctTableNames = new ArrayList(new HashSet(lowercaseTableNames));
                                break;
                            }

                            String tableName = (String)var10.next();
                            lowercaseTableNames.add(org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier(tableName));
                        }
                    }

                    RawStore ms = this.getMS();

                    int endIndex;
                    for(int startIndex = 0; startIndex < ((List)distinctTableNames).size(); startIndex = endIndex) {
                        endIndex = Math.min(startIndex + tableBatchSize, ((List)distinctTableNames).size());
                        tables.addAll(ms.getTableObjectsByName(catName, dbName, ((List)distinctTableNames).subList(startIndex, endIndex)));
                    }

                    Iterator var21 = tables.iterator();

                    while(var21.hasNext()) {
                        Table t = (Table)var21.next();
                        if (MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
                            this.assertClientHasCapability(capabilities, ClientCapability.INSERT_ONLY_TABLES, "insert-only tables", "get_table_req");
                        }
                    }

                    return tables;
                }
            } catch (Exception var16) {
                ex = var16;
                if (var16 instanceof MetaException) {
                    throw (MetaException)var16;
                } else if (var16 instanceof InvalidOperationException) {
                    throw (InvalidOperationException)var16;
                } else if (var16 instanceof UnknownDBException) {
                    throw (UnknownDBException)var16;
                } else {
                    throw newMetaException(var16);
                }
            } finally {
                this.endFunction("get_multi_table", tables != null, ex, org.apache.commons.lang.StringUtils.join(tableNames, ","));
            }
        }

        public Materialization get_materialization_invalidation_info(CreationMetadata cm, String validTxnList) throws MetaException {
            return this.getTxnHandler().getMaterializationInvalidationInfo(cm, validTxnList);
        }

        public void update_creation_metadata(String catName, String dbName, String tableName, CreationMetadata cm) throws MetaException {
            this.getMS().updateCreationMetadata(catName, dbName, tableName, cm);
        }

        private void assertClientHasCapability(ClientCapabilities client, ClientCapability value, String what, String call) throws MetaException {
            if (!this.doesClientHaveCapability(client, value)) {
                throw new MetaException("Your client does not appear to support " + what + ". To skip" + " capability checks, please set " + ConfVars.CAPABILITY_CHECK.toString() + " to false. This setting can be set globally, or on the client for the current" + " metastore session. Note that this may lead to incorrect results, data loss," + " undefined behavior, etc. if your client is actually incompatible. You can also" + " specify custom client capabilities via " + call + " API.");
            }
        }

        private boolean doesClientHaveCapability(ClientCapabilities client, ClientCapability value) {
            if (!MetastoreConf.getBoolVar(this.getConf(), ConfVars.CAPABILITY_CHECK)) {
                return true;
            } else {
                return client != null && client.isSetValues() && client.getValues().contains(value);
            }
        }

        public List<String> get_table_names_by_filter(String dbName, String filter, short maxTables) throws MetaException, InvalidOperationException, UnknownDBException {
            List<String> tables = null;
            this.startFunction("get_table_names_by_filter", ": db = " + dbName + ", filter = " + filter);
            Exception ex = null;
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);

            try {
                if (parsedDbName[0] == null || parsedDbName[0].isEmpty() || parsedDbName[1] == null || parsedDbName[1].isEmpty()) {
                    throw new UnknownDBException("DB name is null or empty");
                }

                if (filter == null) {
                    throw new InvalidOperationException(filter + " cannot apply null filter");
                }

                tables = this.getMS().listTableNamesByFilter(parsedDbName[0], parsedDbName[1], filter, maxTables);
            } catch (Exception var11) {
                ex = var11;
                if (var11 instanceof MetaException) {
                    throw (MetaException)var11;
                }

                if (var11 instanceof InvalidOperationException) {
                    throw (InvalidOperationException)var11;
                }

                if (var11 instanceof UnknownDBException) {
                    throw (UnknownDBException)var11;
                }

                throw newMetaException(var11);
            } finally {
                this.endFunction("get_table_names_by_filter", tables != null, ex, org.apache.commons.lang.StringUtils.join(tables, ","));
            }

            return tables;
        }

        private Partition append_partition_common(RawStore ms, String catName, String dbName, String tableName, List<String> part_vals, EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException, MetaException, NoSuchObjectException {
            Partition part = new Partition();
            boolean success = false;
            boolean madeDir = false;
            Path partLocation = null;
            Table tbl = null;
            Map<String, String> transactionalListenerResponses = Collections.emptyMap();
            Database db = null;

            try {
                ms.openTransaction();
                part.setCatName(catName);
                part.setDbName(dbName);
                part.setTableName(tableName);
                part.setValues(part_vals);
                MetaStoreUtils.validatePartitionNameCharacters(part_vals, this.partitionValidationPattern);
                tbl = ms.getTable(part.getCatName(), part.getDbName(), part.getTableName(), (String)null);
                if (tbl == null) {
                    throw new InvalidObjectException("Unable to add partition because table or database do not exist");
                }

                if (tbl.getSd().getLocation() == null) {
                    throw new MetaException("Cannot append a partition to a view");
                }

                db = this.get_database_core(catName, dbName);
                this.firePreEvent(new PreAddPartitionEvent(tbl, part, this));
                part.setSd(tbl.getSd().deepCopy());
                partLocation = new Path(tbl.getSd().getLocation(), Warehouse.makePartName(tbl.getPartitionKeys(), part_vals));
                part.getSd().setLocation(partLocation.toString());

                Partition old_part;
                try {
                    old_part = ms.getPartition(part.getCatName(), part.getDbName(), part.getTableName(), part.getValues());
                } catch (NoSuchObjectException var20) {
                    old_part = null;
                }

                if (old_part != null) {
                    throw new AlreadyExistsException("Partition already exists:" + part);
                }

                if (!this.wh.isDir(partLocation)) {
                    if (!this.wh.mkdirs(partLocation)) {
                        throw new MetaException(partLocation + " is not a directory or unable to create one");
                    }

                    madeDir = true;
                }

                long time = System.currentTimeMillis() / 1000L;
                part.setCreateTime((int)time);
                part.putToParameters("transient_lastDdlTime", Long.toString(time));
                if (MetastoreConf.getBoolVar(this.conf, ConfVars.STATS_AUTO_GATHER) && !MetaStoreUtils.isView(tbl)) {
                    MetaStoreUtils.updatePartitionStatsFast(part, tbl, this.wh, madeDir, false, envContext, true);
                }

                if (ms.addPartition(part)) {
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, part, true, this), envContext);
                    }

                    success = ms.commitTransaction();
                }
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                    if (madeDir) {
                        this.wh.deleteDir(partLocation, true, db);
                    }
                }

                if (!this.listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, part, success, this), envContext, transactionalListenerResponses, ms);
                }

            }

            return part;
        }

        private void firePreEvent(PreEventContext event) throws MetaException {
            Iterator var2 = this.preListeners.iterator();

            while(var2.hasNext()) {
                MetaStorePreEventListener listener = (MetaStorePreEventListener)var2.next();

                try {
                    listener.onEvent(event);
                } catch (NoSuchObjectException var5) {
                    throw new MetaException(var5.getMessage());
                } catch (InvalidOperationException var6) {
                    throw new MetaException(var6.getMessage());
                }
            }

        }

        public Partition append_partition(String dbName, String tableName, List<String> part_vals) throws InvalidObjectException, AlreadyExistsException, MetaException {
            return this.append_partition_with_environment_context(dbName, tableName, part_vals, (EnvironmentContext)null);
        }

        public Partition append_partition_with_environment_context(String dbName, String tableName, List<String> part_vals, EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException, MetaException {
            if (part_vals != null && !part_vals.isEmpty()) {
                String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
                this.startPartitionFunction("append_partition", parsedDbName[0], parsedDbName[1], tableName, part_vals);
                if (LOG.isDebugEnabled()) {
                    Iterator var6 = part_vals.iterator();

                    while(var6.hasNext()) {
                        String part = (String)var6.next();
                        LOG.debug(part);
                    }
                }

                Partition ret = null;
                Exception ex = null;

                try {
                    ret = this.append_partition_common(this.getMS(), parsedDbName[0], parsedDbName[1], tableName, part_vals, envContext);
                } catch (Exception var12) {
                    ex = var12;
                    if (var12 instanceof MetaException) {
                        throw (MetaException)var12;
                    }

                    if (var12 instanceof InvalidObjectException) {
                        throw (InvalidObjectException)var12;
                    }

                    if (var12 instanceof AlreadyExistsException) {
                        throw (AlreadyExistsException)var12;
                    }

                    throw newMetaException(var12);
                } finally {
                    this.endFunction("append_partition", ret != null, ex, tableName);
                }

                return ret;
            } else {
                throw new MetaException("The partition values must not be null or empty.");
            }
        }

        private List<Partition> add_partitions_core(RawStore ms, String catName, String dbName, String tblName, List<Partition> parts, boolean ifNotExists) throws TException {
            logInfo("add_partitions");
            boolean success = false;
            final Map<HiveMetaStore.HMSHandler.PartValEqWrapper, Boolean> addedPartitions = new ConcurrentHashMap();
            List<Partition> newParts = new ArrayList();
            List<Partition> existingParts = new ArrayList();
            Table tbl = null;
            Map<String, String> transactionalListenerResponses = Collections.emptyMap();
            Database db = null;
            List<ColumnStatistics> partsColStats = new ArrayList(parts.size());
            ArrayList partsWriteIds = new ArrayList(parts.size());
            boolean var31 = false;

            try {
                var31 = true;
                ms.openTransaction();
                tbl = ms.getTable(catName, dbName, tblName);
                if (tbl == null) {
                    throw new InvalidObjectException("Unable to add partitions because " + Warehouse.getCatalogQualifiedTableName(catName, dbName, tblName) + " does not exist");
                }

                db = ms.getDatabase(catName, dbName);
                if (!parts.isEmpty()) {
                    this.firePreEvent(new PreAddPartitionEvent(tbl, parts, this));
                }

                List<Future<Partition>> partFutures = Lists.newArrayList();
                final Table table = tbl;
                Iterator var18 = parts.iterator();

                Iterator var38;
                label272:
                while(true) {
                    if (!var18.hasNext()) {
                        try {
                            var18 = partFutures.iterator();

                            while(true) {
                                if (!var18.hasNext()) {
                                    break label272;
                                }

                                Future<Partition> partFuture = (Future)var18.next();
                                Partition part = (Partition)partFuture.get();
                                if (part != null) {
                                    newParts.add(part);
                                }
                            }
                        } catch (ExecutionException | InterruptedException var33) {
                            var38 = partFutures.iterator();

                            while(var38.hasNext()) {
                                Future<Partition> partFuture = (Future)var38.next();
                                partFuture.cancel(true);
                            }

                            throw new MetaException(var33.getMessage());
                        }
                    }

                    final Partition part = (Partition)var18.next();
                    if (!part.getTableName().equals(tblName) || !part.getDbName().equals(dbName)) {
                        throw new MetaException("Partition does not belong to target table " + Warehouse.getCatalogQualifiedTableName(catName, dbName, tblName) + ": " + part);
                    }

                    if (part.isSetColStats()) {
                        partsColStats.add(part.getColStats());
                        part.unsetColStats();
                        partsWriteIds.add(part.getWriteId());
                    }

                    boolean shouldAdd = this.startAddPartition(ms, part, ifNotExists);
                    if (!shouldAdd) {
                        existingParts.add(part);
                        LOG.info("Not adding partition " + part + " as it already exists");
                    } else {
                        final UserGroupInformation ugi;
                        try {
                            ugi = UserGroupInformation.getCurrentUser();
                        } catch (IOException var32) {
                            throw new RuntimeException(var32);
                        }

                        partFutures.add(threadPool.submit(new Callable<Partition>() {
                            public Partition call() throws Exception {
                                ugi.doAs(new PrivilegedExceptionAction<Object>() {
                                    public Object run() throws Exception {
                                        try {
                                            boolean madeDir = HMSHandler.this.createLocationForAddedPartition(table, part);
                                            if (addedPartitions.put(new HiveMetaStore.HMSHandler.PartValEqWrapper(part), madeDir) != null) {
                                                throw new MetaException("Duplicate partitions in the list: " + part);
                                            } else {
                                                HMSHandler.this.initializeAddedPartition(table, (Partition)part, madeDir, (EnvironmentContext)null);
                                                return null;
                                            }
                                        } catch (MetaException var2) {
                                            throw new IOException(var2.getMessage(), var2);
                                        }
                                    }
                                });
                                return part;
                            }
                        }));
                    }
                }

                if (!newParts.isEmpty()) {
                    ms.addPartitions(catName, dbName, tblName, newParts);
                } else {
                    success = true;
                }

                success = false;
                if (!this.transactionalListeners.isEmpty()) {
                    transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, newParts, true, this));
                }

                if (!this.listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, newParts, true, this), (EnvironmentContext)null, transactionalListenerResponses, ms);
                    if (!existingParts.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, existingParts, false, this), (EnvironmentContext)null, (Map)null, ms);
                    }
                }

                int cnt = 0;
                var38 = partsColStats.iterator();

                while(true) {
                    if (!var38.hasNext()) {
                        success = ms.commitTransaction();
                        var31 = false;
                        break;
                    }

                    ColumnStatistics partColStats = (ColumnStatistics)var38.next();
                    long writeId = (Long)partsWriteIds.get(cnt++);
                    String validWriteIds = null;
                    if (writeId > 0L) {
                        ValidWriteIdList validWriteIdList = new ValidReaderWriteIdList(tbl.getDbName() + "." + tbl.getTableName(), new long[0], new BitSet(), writeId);
                        validWriteIds = validWriteIdList.toString();
                    }

                    this.updatePartitonColStatsInternal(tbl, partColStats, validWriteIds, writeId);
                }
            } finally {
                if (var31) {
                    if (!success) {
                        ms.rollbackTransaction();
                        Iterator var26 = addedPartitions.entrySet().iterator();

                        while(var26.hasNext()) {
                            Entry<HiveMetaStore.HMSHandler.PartValEqWrapper, Boolean> e = (Entry)var26.next();
                            if ((Boolean)e.getValue()) {
                                this.wh.deleteDir(new Path(((HiveMetaStore.HMSHandler.PartValEqWrapper)e.getKey()).partition.getSd().getLocation()), true, db);
                            }
                        }

                        if (!this.listeners.isEmpty()) {
                            MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, parts, false, this), (EnvironmentContext)null, (Map)null, ms);
                        }
                    }

                }
            }

            if (!success) {
                ms.rollbackTransaction();
                Iterator var35 = addedPartitions.entrySet().iterator();

                while(var35.hasNext()) {
                    Entry<HiveMetaStore.HMSHandler.PartValEqWrapper, Boolean> e = (Entry)var35.next();
                    if ((Boolean)e.getValue()) {
                        this.wh.deleteDir(new Path(((HiveMetaStore.HMSHandler.PartValEqWrapper)e.getKey()).partition.getSd().getLocation()), true, db);
                    }
                }

                if (!this.listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, parts, false, this), (EnvironmentContext)null, (Map)null, ms);
                }
            }

            return newParts;
        }

        public AddPartitionsResult add_partitions_req(AddPartitionsRequest request) throws TException {
            AddPartitionsResult result = new AddPartitionsResult();
            if (request.getParts().isEmpty()) {
                return result;
            } else {
                try {
                    if (!request.isSetCatName()) {
                        request.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
                    }

                    request.getParts().forEach((p) -> {
                        if (!p.isSetCatName()) {
                            p.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
                        }

                    });
                    List<Partition> parts = this.add_partitions_core(this.getMS(), request.getCatName(), request.getDbName(), request.getTblName(), request.getParts(), request.isIfNotExists());
                    if (request.isNeedResult()) {
                        result.setPartitions(parts);
                    }

                    return result;
                } catch (TException var4) {
                    throw var4;
                } catch (Exception var5) {
                    throw newMetaException(var5);
                }
            }
        }

        public int add_partitions(List<Partition> parts) throws MetaException, InvalidObjectException, AlreadyExistsException {
            this.startFunction("add_partition");
            if (parts.size() == 0) {
                return 0;
            } else {
                Integer ret = null;
                Exception ex = null;
                boolean var11 = false;

                String defaultCat;
                try {
                    var11 = true;
                    if (!((Partition)parts.get(0)).isSetCatName()) {
                        defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
                        Iterator var5 = parts.iterator();

                        while(var5.hasNext()) {
                            Partition p = (Partition)var5.next();
                            p.setCatName(defaultCat);
                        }
                    }

                    ret = this.add_partitions_core(this.getMS(), ((Partition)parts.get(0)).getCatName(), ((Partition)parts.get(0)).getDbName(), ((Partition)parts.get(0)).getTableName(), parts, false).size();
                    assert ret == parts.size();
                } catch (Exception var12) {
                    ex = var12;
                    if (var12 instanceof MetaException) {
                        throw (MetaException)var12;
                    }

                    if (var12 instanceof InvalidObjectException) {
                        throw (InvalidObjectException)var12;
                    }

                    if (var12 instanceof AlreadyExistsException) {
                        throw (AlreadyExistsException)var12;
                    }

                    throw newMetaException(var12);
                } finally {
                    if (var11) {
                        String tableName = ((Partition)parts.get(0)).getTableName();
                        this.endFunction("add_partition", ret != null, ex, tableName);
                    }
                }

                defaultCat = ((Partition)parts.get(0)).getTableName();
                this.endFunction("add_partition", ret != null, ex, defaultCat);
                return ret;
            }
        }

        public int add_partitions_pspec(List<PartitionSpec> partSpecs) throws TException {
            logInfo("add_partitions_pspec");
            if (partSpecs.isEmpty()) {
                return 0;
            } else {
                String dbName = ((PartitionSpec)partSpecs.get(0)).getDbName();
                String tableName = ((PartitionSpec)partSpecs.get(0)).getTableName();
                String catName;
                if (!((PartitionSpec)partSpecs.get(0)).isSetCatName()) {
                    catName = MetaStoreUtils.getDefaultCatalog(this.conf);
                    partSpecs.forEach((ps) -> {
                        ps.setCatName(catName);
                    });
                } else {
                    catName = ((PartitionSpec)partSpecs.get(0)).getCatName();
                }

                return this.add_partitions_pspec_core(this.getMS(), catName, dbName, tableName, partSpecs, false);
            }
        }

        private int add_partitions_pspec_core(RawStore ms, String catName, String dbName, String tblName, List<PartitionSpec> partSpecs, boolean ifNotExists) throws TException {
            boolean success = false;
            final Map<HiveMetaStore.HMSHandler.PartValEqWrapperLite, Boolean> addedPartitions = new ConcurrentHashMap();
            PartitionSpecProxy partitionSpecProxy = org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy.Factory.get(partSpecs);
            PartitionIterator partitionIterator = partitionSpecProxy.getPartitionIterator();
            Table tbl = null;
            Map<String, String> transactionalListenerResponses = Collections.emptyMap();
            Database db = null;
            boolean var26 = false;

            Iterator var17;
            int var30;
            try {
                var26 = true;
                ms.openTransaction();
                tbl = ms.getTable(catName, dbName, tblName, (String)null);
                if (tbl == null) {
                    throw new InvalidObjectException("Unable to add partitions because database or table " + dbName + "." + tblName + " does not exist");
                }

                db = ms.getDatabase(catName, dbName);
                this.firePreEvent(new PreAddPartitionEvent(tbl, partitionSpecProxy, this));
                List<Future<Partition>> partFutures = Lists.newArrayList();
                final Table table = tbl;

                while(true) {
                    if (partitionIterator.hasNext()) {
                        final Partition part = partitionIterator.getCurrent();
                        if (part.getTableName().equalsIgnoreCase(tblName) && part.getDbName().equalsIgnoreCase(dbName)) {
                            boolean shouldAdd = this.startAddPartition(ms, part, ifNotExists);
                            if (!shouldAdd) {
                                LOG.info("Not adding partition " + part + " as it already exists");
                            } else {
                                final UserGroupInformation ugi;
                                try {
                                    ugi = UserGroupInformation.getCurrentUser();
                                } catch (IOException var27) {
                                    throw new RuntimeException(var27);
                                }

                                partFutures.add(threadPool.submit(new Callable<Partition>() {
                                    public Partition call() throws Exception {
                                        ugi.doAs(new PrivilegedExceptionAction<Partition>() {
                                            public Partition run() throws Exception {
                                                try {
                                                    boolean madeDir = HMSHandler.this.createLocationForAddedPartition(table, part);
                                                    if (addedPartitions.put(new HiveMetaStore.HMSHandler.PartValEqWrapperLite(part), madeDir) != null) {
                                                        throw new MetaException("Duplicate partitions in the list: " + part);
                                                    } else {
                                                        HMSHandler.this.initializeAddedPartition(table, (Partition)part, madeDir, (EnvironmentContext)null);
                                                        return null;
                                                    }
                                                } catch (MetaException var2) {
                                                    throw new IOException(var2.getMessage(), var2);
                                                }
                                            }
                                        });
                                        return part;
                                    }
                                }));
                                partitionIterator.next();
                            }
                            continue;
                        }

                        throw new MetaException("Partition does not belong to target table " + dbName + "." + tblName + ": " + part);
                    }

                    try {
                        Iterator var16 = partFutures.iterator();

                        while(var16.hasNext()) {
                            Future<Partition> partFuture = (Future)var16.next();
                            partFuture.get();
                        }
                    } catch (ExecutionException | InterruptedException var28) {
                        var17 = partFutures.iterator();

                        while(var17.hasNext()) {
                            Future<Partition> partFuture = (Future)var17.next();
                            partFuture.cancel(true);
                        }

                        throw new MetaException(var28.getMessage());
                    }

                    ms.addPartitions(catName, dbName, tblName, partitionSpecProxy, ifNotExists);
                    success = false;
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, partitionSpecProxy, true, this));
                    }

                    success = ms.commitTransaction();
                    var30 = addedPartitions.size();
                    var26 = false;
                    break;
                }
            } finally {
                if (var26) {
                    if (!success) {
                        ms.rollbackTransaction();
                        Iterator var21 = addedPartitions.entrySet().iterator();

                        while(var21.hasNext()) {
                            Entry<HiveMetaStore.HMSHandler.PartValEqWrapperLite, Boolean> e = (Entry)var21.next();
                            if ((Boolean)e.getValue()) {
                                this.wh.deleteDir(new Path(((HiveMetaStore.HMSHandler.PartValEqWrapperLite)e.getKey()).location), true, db);
                            }
                        }
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, partitionSpecProxy, true, this), (EnvironmentContext)null, transactionalListenerResponses, ms);
                    }

                }
            }

            if (!success) {
                ms.rollbackTransaction();
                var17 = addedPartitions.entrySet().iterator();

                while(var17.hasNext()) {
                    Entry<HiveMetaStore.HMSHandler.PartValEqWrapperLite, Boolean> e = (Entry)var17.next();
                    if ((Boolean)e.getValue()) {
                        this.wh.deleteDir(new Path(((HiveMetaStore.HMSHandler.PartValEqWrapperLite)e.getKey()).location), true, db);
                    }
                }
            }

            if (!this.listeners.isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, partitionSpecProxy, true, this), (EnvironmentContext)null, transactionalListenerResponses, ms);
            }

            return var30;
        }

        private boolean startAddPartition(RawStore ms, Partition part, boolean ifNotExists) throws TException {
            MetaStoreUtils.validatePartitionNameCharacters(part.getValues(), this.partitionValidationPattern);
            boolean doesExist = ms.doesPartitionExist(part.getCatName(), part.getDbName(), part.getTableName(), part.getValues());
            if (doesExist && !ifNotExists) {
                throw new AlreadyExistsException("Partition already exists: " + part);
            } else {
                return !doesExist;
            }
        }

        private boolean createLocationForAddedPartition(Table tbl, Partition part) throws MetaException {
            Path partLocation = null;
            String partLocationStr = null;
            if (part.getSd() != null) {
                partLocationStr = part.getSd().getLocation();
            }

            if (partLocationStr != null && !partLocationStr.isEmpty()) {
                if (tbl.getSd().getLocation() == null) {
                    throw new MetaException("Cannot specify location for a view partition");
                }

                partLocation = this.wh.getDnsPath(new Path(partLocationStr));
            } else if (tbl.getSd().getLocation() != null) {
                partLocation = new Path(tbl.getSd().getLocation(), Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues()));
            }

            boolean result = false;
            if (partLocation != null) {
                part.getSd().setLocation(partLocation.toString());
                if (!this.wh.isDir(partLocation)) {
                    if (!this.wh.mkdirs(partLocation)) {
                        throw new MetaException(partLocation + " is not a directory or unable to create one");
                    }

                    result = true;
                }
            }

            return result;
        }

        private void initializeAddedPartition(Table tbl, Partition part, boolean madeDir, EnvironmentContext environmentContext) throws MetaException {
            this.initializeAddedPartition(tbl, (PartitionIterator)(new SimplePartitionWrapperIterator(part)), madeDir, environmentContext);
        }

        private void initializeAddedPartition(Table tbl, PartitionIterator part, boolean madeDir, EnvironmentContext environmentContext) throws MetaException {
            if (MetastoreConf.getBoolVar(this.conf, ConfVars.STATS_AUTO_GATHER) && !MetaStoreUtils.isView(tbl)) {
                MetaStoreUtils.updatePartitionStatsFast(part, tbl, this.wh, madeDir, false, environmentContext, true);
            }

            long time = System.currentTimeMillis() / 1000L;
            part.setCreateTime((long)((int)time));
            if (part.getParameters() == null || part.getParameters().get("transient_lastDdlTime") == null) {
                part.putToParameters("transient_lastDdlTime", Long.toString(time));
            }

            Map<String, String> tblParams = tbl.getParameters();
            String inheritProps = MetastoreConf.getVar(this.conf, ConfVars.PART_INHERIT_TBL_PROPS).trim();
            Set<String> inheritKeys = new HashSet(Arrays.asList(inheritProps.split(",")));
            if (((Set)inheritKeys).contains("*")) {
                inheritKeys = tblParams.keySet();
            }

            Iterator var10 = ((Set)inheritKeys).iterator();

            while(var10.hasNext()) {
                String key = (String)var10.next();
                String paramVal = (String)tblParams.get(key);
                if (null != paramVal) {
                    part.putToParameters(key, paramVal);
                }
            }

        }

        private Partition add_partition_core(RawStore ms, Partition part, EnvironmentContext envContext) throws TException {
            boolean success = false;
            Table tbl = null;
            Map<String, String> transactionalListenerResponses = Collections.emptyMap();
            if (!part.isSetCatName()) {
                part.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
            }

            try {
                ms.openTransaction();
                tbl = ms.getTable(part.getCatName(), part.getDbName(), part.getTableName(), (String)null);
                if (tbl == null) {
                    throw new InvalidObjectException("Unable to add partition because table or database do not exist");
                }

                this.firePreEvent(new PreAddPartitionEvent(tbl, part, this));
                boolean shouldAdd = this.startAddPartition(ms, part, false);

                assert shouldAdd;

                boolean madeDir = this.createLocationForAddedPartition(tbl, part);

                try {
                    this.initializeAddedPartition(tbl, part, madeDir, envContext);
                    success = ms.addPartition(part);
                } finally {
                    if (true) {
                        this.wh.deleteDir(new Path(part.getSd().getLocation()), true, ms.getDatabase(tbl.getCatName(), tbl.getDbName()));
                    }

                }

                success = false;
                if (!this.transactionalListeners.isEmpty()) {
                    transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, Arrays.asList(part), true, this), envContext);
                }

                success = ms.commitTransaction();
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                }

                if (!this.listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_PARTITION, new AddPartitionEvent(tbl, Arrays.asList(part), success, this), envContext, transactionalListenerResponses, ms);
                }

            }

            return part;
        }

        public Partition add_partition(Partition part) throws InvalidObjectException, AlreadyExistsException, MetaException {
            return this.add_partition_with_environment_context(part, (EnvironmentContext)null);
        }

        public Partition add_partition_with_environment_context(Partition part, EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException, MetaException {
            this.startTableFunction("add_partition", part.getCatName(), part.getDbName(), part.getTableName());
            Partition ret = null;
            Exception ex = null;

            try {
                ret = this.add_partition_core(this.getMS(), part, envContext);
            } catch (Exception var9) {
                ex = var9;
                if (var9 instanceof MetaException) {
                    throw (MetaException)var9;
                }

                if (var9 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var9;
                }

                if (var9 instanceof AlreadyExistsException) {
                    throw (AlreadyExistsException)var9;
                }

                throw newMetaException(var9);
            } finally {
                this.endFunction("add_partition", ret != null, ex, part != null ? part.getTableName() : null);
            }

            return ret;
        }

        public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceDbName, String sourceTableName, String destDbName, String destTableName) throws TException {
            this.exchange_partitions(partitionSpecs, sourceDbName, sourceTableName, destDbName, destTableName);
            return new Partition();
        }

        public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceDbName, String sourceTableName, String destDbName, String destTableName) throws TException {
            String[] parsedDestDbName = parseDbName(destDbName, conf);
            String[] parsedSourceDbName = parseDbName(sourceDbName, conf);
            // No need to check catalog for null as parseDbName() will never return null for the catalog.
            if (partitionSpecs == null || parsedSourceDbName[DB_NAME] == null || sourceTableName == null
                    || parsedDestDbName[DB_NAME] == null || destTableName == null) {
                throw new MetaException("The DB and table name for the source and destination tables,"
                        + " and the partition specs must not be null.");
            }
            if (!parsedDestDbName[CAT_NAME].equals(parsedSourceDbName[CAT_NAME])) {
                throw new MetaException("You cannot move a partition across catalogs");
            }

            boolean success = false;
            boolean pathCreated = false;
            RawStore ms = getMS();
            ms.openTransaction();

            Table destinationTable =
                    ms.getTable(parsedDestDbName[CAT_NAME], parsedDestDbName[DB_NAME], destTableName);
            if (destinationTable == null) {
                throw new MetaException( "The destination table " +
                        getCatalogQualifiedTableName(parsedDestDbName[CAT_NAME],
                                parsedDestDbName[DB_NAME], destTableName) + " not found");
            }
            Table sourceTable =
                    ms.getTable(parsedSourceDbName[CAT_NAME], parsedSourceDbName[DB_NAME], sourceTableName);
            if (sourceTable == null) {
                throw new MetaException("The source table " +
                        getCatalogQualifiedTableName(parsedSourceDbName[CAT_NAME],
                                parsedSourceDbName[DB_NAME], sourceTableName) + " not found");
            }
            List<String> partVals = MetaStoreUtils.getPvals(sourceTable.getPartitionKeys(),
                    partitionSpecs);
            List<String> partValsPresent = new ArrayList<> ();
            List<FieldSchema> partitionKeysPresent = new ArrayList<> ();
            int i = 0;
            for (FieldSchema fs: sourceTable.getPartitionKeys()) {
                String partVal = partVals.get(i);
                if (partVal != null && !partVal.equals("")) {
                    partValsPresent.add(partVal);
                    partitionKeysPresent.add(fs);
                }
                i++;
            }
            // Passed the unparsed DB name here, as get_partitions_ps expects to parse it
            List<Partition> partitionsToExchange = get_partitions_ps(sourceDbName, sourceTableName,
                    partVals, (short)-1);
            if (partitionsToExchange == null || partitionsToExchange.isEmpty()) {
                throw new MetaException("No partition is found with the values " + partitionSpecs
                        + " for the table " + sourceTableName);
            }
            boolean sameColumns = MetaStoreUtils.compareFieldColumns(
                    sourceTable.getSd().getCols(), destinationTable.getSd().getCols());
            boolean samePartitions = MetaStoreUtils.compareFieldColumns(
                    sourceTable.getPartitionKeys(), destinationTable.getPartitionKeys());
            if (!sameColumns || !samePartitions) {
                throw new MetaException("The tables have different schemas." +
                        " Their partitions cannot be exchanged.");
            }
            Path sourcePath = new Path(sourceTable.getSd().getLocation(),
                    Warehouse.makePartName(partitionKeysPresent, partValsPresent));
            Path destPath = new Path(destinationTable.getSd().getLocation(),
                    Warehouse.makePartName(partitionKeysPresent, partValsPresent));
            List<Partition> destPartitions = new ArrayList<>();

            Map<String, String> transactionalListenerResponsesForAddPartition = Collections.emptyMap();
            List<Map<String, String>> transactionalListenerResponsesForDropPartition =
                    Lists.newArrayListWithCapacity(partitionsToExchange.size());

            // Check if any of the partitions already exists in destTable.
            List<String> destPartitionNames = ms.listPartitionNames(parsedDestDbName[CAT_NAME],
                    parsedDestDbName[DB_NAME], destTableName, (short) -1);
            if (destPartitionNames != null && !destPartitionNames.isEmpty()) {
                for (Partition partition : partitionsToExchange) {
                    String partToExchangeName =
                            Warehouse.makePartName(destinationTable.getPartitionKeys(), partition.getValues());
                    if (destPartitionNames.contains(partToExchangeName)) {
                        throw new MetaException("The partition " + partToExchangeName
                                + " already exists in the table " + destTableName);
                    }
                }
            }

            try {
                for (Partition partition: partitionsToExchange) {
                    Partition destPartition = new Partition(partition);
                    destPartition.setDbName(parsedDestDbName[DB_NAME]);
                    destPartition.setTableName(destinationTable.getTableName());
                    Path destPartitionPath = new Path(destinationTable.getSd().getLocation(),
                            Warehouse.makePartName(destinationTable.getPartitionKeys(), partition.getValues()));
                    destPartition.getSd().setLocation(destPartitionPath.toString());
                    ms.addPartition(destPartition);
                    destPartitions.add(destPartition);
                    ms.dropPartition(parsedSourceDbName[CAT_NAME], partition.getDbName(), sourceTable.getTableName(),
                            partition.getValues());
                }
                Path destParentPath = destPath.getParent();
                if (!wh.isDir(destParentPath)) {
                    if (!wh.mkdirs(destParentPath)) {
                        throw new MetaException("Unable to create path " + destParentPath);
                    }
                }
                /*
                 * TODO: Use the hard link feature of hdfs
                 * once https://issues.apache.org/jira/browse/HDFS-3370 is done
                 */
                pathCreated = wh.renameDir(sourcePath, destPath, false);

                // Setting success to false to make sure that if the listener fails, rollback happens.
                success = false;

                if (!transactionalListeners.isEmpty()) {
                    transactionalListenerResponsesForAddPartition =
                            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                    EventType.ADD_PARTITION,
                                    new AddPartitionEvent(destinationTable, destPartitions, true, this));

                    for (Partition partition : partitionsToExchange) {
                        DropPartitionEvent dropPartitionEvent =
                                new DropPartitionEvent(sourceTable, partition, true, true, this);
                        transactionalListenerResponsesForDropPartition.add(
                                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                        EventType.DROP_PARTITION,
                                        dropPartitionEvent));
                    }
                }

                success = ms.commitTransaction();
                return destPartitions;
            } finally {
                if (!success || !pathCreated) {
                    ms.rollbackTransaction();
                    if (pathCreated) {
                        wh.renameDir(destPath, sourcePath, false);
                    }
                }

                if (!listeners.isEmpty()) {
                    AddPartitionEvent addPartitionEvent = new AddPartitionEvent(destinationTable, destPartitions, success, this);
                    MetaStoreListenerNotifier.notifyEvent(listeners,
                            EventType.ADD_PARTITION,
                            addPartitionEvent,
                            null,
                            transactionalListenerResponsesForAddPartition, ms);

                    i = 0;
                    for (Partition partition : partitionsToExchange) {
                        DropPartitionEvent dropPartitionEvent =
                                new DropPartitionEvent(sourceTable, partition, success, true, this);
                        Map<String, String> parameters =
                                (transactionalListenerResponsesForDropPartition.size() > i)
                                        ? transactionalListenerResponsesForDropPartition.get(i)
                                        : null;

                        MetaStoreListenerNotifier.notifyEvent(listeners,
                                EventType.DROP_PARTITION,
                                dropPartitionEvent,
                                null,
                                parameters, ms);
                        i++;
                    }
                }
            }
        }

        private boolean drop_partition_common(RawStore ms, String catName, String db_name, String tbl_name, List<String> part_vals, boolean deleteData, EnvironmentContext envContext) throws MetaException, NoSuchObjectException, IOException, InvalidObjectException, InvalidInputException {
            boolean success = false;
            Path partPath = null;
            Table tbl = null;
            Partition part = null;
            boolean isArchived = false;
            Path archiveParentDir = null;
            boolean mustPurge = false;
            boolean tableDataShouldBeDeleted = false;
            boolean isSourceOfReplication = false;
            Map<String, String> transactionalListenerResponses = Collections.emptyMap();
            if (db_name == null) {
                throw new MetaException("The DB name cannot be null.");
            } else if (tbl_name == null) {
                throw new MetaException("The table name cannot be null.");
            } else if (part_vals == null) {
                throw new MetaException("The partition values cannot be null.");
            } else {
                try {
                    ms.openTransaction();
                    part = ms.getPartition(catName, db_name, tbl_name, part_vals);
                    tbl = this.get_table_core(catName, db_name, tbl_name, (String)null);
                    tableDataShouldBeDeleted = this.checkTableDataShouldBeDeleted(tbl, deleteData);
                    this.firePreEvent(new PreDropPartitionEvent(tbl, part, deleteData, this));
                    mustPurge = isMustPurge(envContext, tbl);
                    if (part == null) {
                        throw new NoSuchObjectException("Partition doesn't exist. " + part_vals);
                    }

                    isArchived = MetaStoreUtils.isArchived(part);
                    if (isArchived) {
                        archiveParentDir = MetaStoreUtils.getOriginalLocation(part);
                        this.verifyIsWritablePath(archiveParentDir);
                    }

                    if (part.getSd() != null && part.getSd().getLocation() != null) {
                        partPath = new Path(part.getSd().getLocation());
                        this.verifyIsWritablePath(partPath);
                    }

                    if (!ms.dropPartition(catName, db_name, tbl_name, part_vals)) {
                        throw new MetaException("Unable to drop partition");
                    }

                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.DROP_PARTITION, new DropPartitionEvent(tbl, part, true, deleteData, this), envContext);
                    }

                    isSourceOfReplication = ReplChangeManager.isSourceOfReplication(ms.getDatabase(catName, db_name));
                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                    } else if (deleteData && (partPath != null || archiveParentDir != null) && tableDataShouldBeDeleted) {
                        if (mustPurge) {
                            LOG.info("dropPartition() will purge " + partPath + " directly, skipping trash.");
                        } else {
                            LOG.info("dropPartition() will move " + partPath + " to trash-directory.");
                        }

                        if (isArchived) {
                            assert archiveParentDir != null;

                            this.wh.deleteDir(archiveParentDir, true, mustPurge, isSourceOfReplication);
                        } else {
                            assert partPath != null;

                            this.wh.deleteDir(partPath, true, mustPurge, isSourceOfReplication);
                            this.deleteParentRecursive(partPath.getParent(), part_vals.size() - 1, mustPurge, isSourceOfReplication);
                        }
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.DROP_PARTITION, new DropPartitionEvent(tbl, part, success, deleteData, this), envContext, transactionalListenerResponses, ms);
                    }

                }

                return true;
            }
        }

        private static boolean isMustPurge(EnvironmentContext envContext, Table tbl) {
            return envContext != null && Boolean.parseBoolean((String)envContext.getProperties().get("ifPurge")) || tbl.isSetParameters() && "true".equalsIgnoreCase((String)tbl.getParameters().get("auto.purge"));
        }

        private void deleteParentRecursive(Path parent, int depth, boolean mustPurge, boolean needRecycle) throws IOException, MetaException {
            if (depth > 0 && parent != null && this.wh.isWritable(parent)) {
                if (this.wh.isDir(parent) && this.wh.isEmpty(parent)) {
                    this.wh.deleteDir(parent, true, mustPurge, needRecycle);
                }

                this.deleteParentRecursive(parent.getParent(), depth - 1, mustPurge, needRecycle);
            }

        }

        public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws TException {
            return this.drop_partition_with_environment_context(db_name, tbl_name, part_vals, deleteData, (EnvironmentContext)null);
        }

        public DropPartitionsResult drop_partitions_req(DropPartitionsRequest request) throws TException {
            RawStore ms = getMS();
            String dbName = request.getDbName(), tblName = request.getTblName();
            String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
            boolean ifExists = request.isSetIfExists() && request.isIfExists();
            boolean deleteData = request.isSetDeleteData() && request.isDeleteData();
            boolean ignoreProtection = request.isSetIgnoreProtection() && request.isIgnoreProtection();
            boolean needResult = !request.isSetNeedResult() || request.isNeedResult();
            List<PathAndPartValSize> dirsToDelete = new ArrayList<>();
            List<Path> archToDelete = new ArrayList<>();
            EnvironmentContext envContext = request.isSetEnvironmentContext()
                    ? request.getEnvironmentContext() : null;

            boolean success = false;
            ms.openTransaction();
            Table tbl = null;
            List<Partition> parts = null;
            boolean mustPurge = false;
            List<Map<String, String>> transactionalListenerResponses = Lists.newArrayList();
            boolean isSourceOfReplication = ReplChangeManager.isSourceOfReplication(ms.getDatabase(catName, dbName));

            try {
                // We need Partition-s for firing events and for result; DN needs MPartition-s to drop.
                // Great... Maybe we could bypass fetching MPartitions by issuing direct SQL deletes.
                tbl = get_table_core(catName, dbName, tblName);
                isExternal(tbl);
                mustPurge = isMustPurge(envContext, tbl);
                int minCount = 0;
                RequestPartsSpec spec = request.getParts();
                List<String> partNames = null;
                if (spec.isSetExprs()) {
                    // Dropping by expressions.
                    parts = new ArrayList<>(spec.getExprs().size());
                    for (DropPartitionsExpr expr : spec.getExprs()) {
                        ++minCount; // At least one partition per expression, if not ifExists
                        List<Partition> result = new ArrayList<>();
                        boolean hasUnknown = ms.getPartitionsByExpr(
                                catName, dbName, tblName, expr.getExpr(), null, (short)-1, result);
                        if (hasUnknown) {
                            // Expr is built by DDLSA, it should only contain part cols and simple ops
                            throw new MetaException("Unexpected unknown partitions to drop");
                        }
                        // this is to prevent dropping archived partition which is archived in a
                        // different level the drop command specified.
                        if (!ignoreProtection && expr.isSetPartArchiveLevel()) {
                            for (Partition part : parts) {
                                if (MetaStoreUtils.isArchived(part)
                                        && MetaStoreUtils.getArchivingLevel(part) < expr.getPartArchiveLevel()) {
                                    throw new MetaException("Cannot drop a subset of partitions "
                                            + " in an archive, partition " + part);
                                }
                            }
                        }
                        parts.addAll(result);
                    }
                } else if (spec.isSetNames()) {
                    partNames = spec.getNames();
                    minCount = partNames.size();
                    parts = ms.getPartitionsByNames(catName, dbName, tblName, partNames);
                } else {
                    throw new MetaException("Partition spec is not set");
                }

                if ((parts.size() < minCount) && !ifExists) {
                    throw new NoSuchObjectException("Some partitions to drop are missing");
                }

                List<String> colNames = null;
                if (partNames == null) {
                    partNames = new ArrayList<>(parts.size());
                    colNames = new ArrayList<>(tbl.getPartitionKeys().size());
                    for (FieldSchema col : tbl.getPartitionKeys()) {
                        colNames.add(col.getName());
                    }
                }

                for (Partition part : parts) {

                    // TODO - we need to speed this up for the normal path where all partitions are under
                    // the table and we don't have to stat every partition

                    firePreEvent(new PreDropPartitionEvent(tbl, part, deleteData, this));
                    if (colNames != null) {
                        partNames.add(FileUtils.makePartName(colNames, part.getValues()));
                    }
                    // Preserve the old behavior of failing when we cannot write, even w/o deleteData,
                    // and even if the table is external. That might not make any sense.
                    if (MetaStoreUtils.isArchived(part)) {
                        Path archiveParentDir = MetaStoreUtils.getOriginalLocation(part);
                        verifyIsWritablePath(archiveParentDir);
                        archToDelete.add(archiveParentDir);
                    }
                    if ((part.getSd() != null) && (part.getSd().getLocation() != null)) {
                        Path partPath = new Path(part.getSd().getLocation());
                        verifyIsWritablePath(partPath);
                        dirsToDelete.add(new PathAndPartValSize(partPath, part.getValues().size()));
                    }
                }

                ms.dropPartitions(catName, dbName, tblName, partNames);
                if (parts != null && !transactionalListeners.isEmpty()) {
                    for (Partition part : parts) {
                        transactionalListenerResponses.add(
                                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                        EventType.DROP_PARTITION,
                                        new DropPartitionEvent(tbl, part, true, deleteData, this),
                                        envContext));
                    }
                }

                success = ms.commitTransaction();
                DropPartitionsResult result = new DropPartitionsResult();
                if (needResult) {
                    result.setPartitions(parts);
                }

                return result;
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                } else if (deleteData && !isExternal(tbl)) {
                    LOG.info( mustPurge?
                            "dropPartition() will purge partition-directories directly, skipping trash."
                            :  "dropPartition() will move partition-directories to trash-directory.");
                    // Archived partitions have har:/to_har_file as their location.
                    // The original directory was saved in params
                    for (Path path : archToDelete) {
                        wh.deleteDir(path, true, mustPurge, isSourceOfReplication);
                    }
                    for (PathAndPartValSize p : dirsToDelete) {
                        wh.deleteDir(p.path, true, mustPurge, isSourceOfReplication);
                        try {
                            deleteParentRecursive(p.path.getParent(), p.partValSize - 1, mustPurge, isSourceOfReplication);
                        } catch (IOException ex) {
                            LOG.warn("Error from deleteParentRecursive", ex);
                            throw new MetaException("Failed to delete parent: " + ex.getMessage());
                        }
                    }
                }
                if (parts != null) {
                    int i = 0;
                    if (parts != null && !listeners.isEmpty()) {
                        for (Partition part : parts) {
                            Map<String, String> parameters =
                                    (!transactionalListenerResponses.isEmpty()) ? transactionalListenerResponses.get(i) : null;

                            MetaStoreListenerNotifier.notifyEvent(listeners,
                                    EventType.DROP_PARTITION,
                                    new DropPartitionEvent(tbl, part, success, deleteData, this),
                                    envContext,
                                    parameters, ms);

                            i++;
                        }
                    }
                }
            }
        }

        private void verifyIsWritablePath(Path dir) throws MetaException {
            try {
                if (!this.wh.isWritable(dir.getParent())) {
                    throw new MetaException("Table partition not deleted since " + dir.getParent() + " is not writable by " + SecurityUtils.getUser());
                }
            } catch (IOException var3) {
                LOG.warn("Error from isWritable", var3);
                throw new MetaException("Table partition not deleted since " + dir.getParent() + " access cannot be checked: " + var3.getMessage());
            }
        }

        public boolean drop_partition_with_environment_context(String db_name, String tbl_name, List<String> part_vals, boolean deleteData, EnvironmentContext envContext) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startPartitionFunction("drop_partition", parsedDbName[0], parsedDbName[1], tbl_name, part_vals);
            LOG.info("Partition values:" + part_vals);
            boolean ret = false;
            Object ex = null;

            try {
                ret = this.drop_partition_common(this.getMS(), parsedDbName[0], parsedDbName[1], tbl_name, part_vals, deleteData, envContext);
            } catch (IOException var14) {
                ex = var14;
                throw new MetaException(var14.getMessage());
            } catch (Exception var15) {
                ex = var15;
                this.rethrowException(var15);
            } finally {
                this.endFunction("drop_partition", ret, (Exception)ex, tbl_name);
            }

            return ret;
        }

        public Partition get_partition(String db_name, String tbl_name, List<String> part_vals) throws MetaException, NoSuchObjectException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startPartitionFunction("get_partition", parsedDbName[0], parsedDbName[1], tbl_name, part_vals);
            Partition ret = null;
            Exception ex = null;

            try {
                this.fireReadTablePreEvent(parsedDbName[0], parsedDbName[1], tbl_name);
                ret = this.getMS().getPartition(parsedDbName[0], parsedDbName[1], tbl_name, part_vals);
            } catch (Exception var11) {
                ex = var11;
                this.throwMetaException(var11);
            } finally {
                this.endFunction("get_partition", ret != null, ex, tbl_name);
            }

            return ret;
        }

        private void fireReadTablePreEvent(String catName, String dbName, String tblName) throws MetaException, NoSuchObjectException {
            if (this.preListeners.size() > 0) {
                Table t = this.getMS().getTable(catName, dbName, tblName);
                if (t == null) {
                    throw new NoSuchObjectException(Warehouse.getCatalogQualifiedTableName(catName, dbName, tblName) + " table not found");
                }

                this.firePreEvent(new PreReadTableEvent(t, this));
            }

        }

        public Partition get_partition_with_auth(String db_name, String tbl_name, List<String> part_vals, String user_name, List<String> group_names) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startPartitionFunction("get_partition_with_auth", parsedDbName[0], parsedDbName[1], tbl_name, part_vals);
            this.fireReadTablePreEvent(parsedDbName[0], parsedDbName[1], tbl_name);
            Partition ret = null;
            Object ex = null;

            try {
                ret = this.getMS().getPartitionWithAuth(parsedDbName[0], parsedDbName[1], tbl_name, part_vals, user_name, group_names);
            } catch (InvalidObjectException var14) {
                ex = var14;
                throw new NoSuchObjectException(var14.getMessage());
            } catch (Exception var15) {
                ex = var15;
                this.rethrowException(var15);
            } finally {
                this.endFunction("get_partition_with_auth", ret != null, (Exception)ex, tbl_name);
            }

            return ret;
        }

        public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts) throws NoSuchObjectException, MetaException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startTableFunction("get_partitions", parsedDbName[0], parsedDbName[1], tbl_name);
            this.fireReadTablePreEvent(parsedDbName[0], parsedDbName[1], tbl_name);
            List<Partition> ret = null;
            Exception ex = null;

            try {
                this.checkLimitNumberOfPartitionsByFilter(parsedDbName[0], parsedDbName[1], tbl_name, "", max_parts);
                ret = this.getMS().getPartitions(parsedDbName[0], parsedDbName[1], tbl_name, max_parts);
            } catch (Exception var11) {
                ex = var11;
                this.throwMetaException(var11);
            } finally {
                this.endFunction("get_partitions", ret != null, ex, tbl_name);
            }

            return ret;
        }

        public List<Partition> get_partitions_with_auth(String dbName, String tblName, short maxParts, String userName, List<String> groupNames) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            this.startTableFunction("get_partitions_with_auth", parsedDbName[0], parsedDbName[1], tblName);
            List<Partition> ret = null;
            Object ex = null;

            try {
                this.checkLimitNumberOfPartitionsByFilter(parsedDbName[0], parsedDbName[1], tblName, "", maxParts);
                ret = this.getMS().getPartitionsWithAuth(parsedDbName[0], parsedDbName[1], tblName, maxParts, userName, groupNames);
            } catch (InvalidObjectException var14) {
                ex = var14;
                throw new NoSuchObjectException(var14.getMessage());
            } catch (Exception var15) {
                ex = var15;
                this.rethrowException(var15);
            } finally {
                this.endFunction("get_partitions_with_auth", ret != null, (Exception)ex, tblName);
            }

            return ret;
        }

        private void checkLimitNumberOfPartitionsByFilter(String catName, String dbName, String tblName, String filterString, int maxParts) throws TException {
            if (this.isPartitionLimitEnabled()) {
                this.checkLimitNumberOfPartitions(tblName, this.get_num_partitions_by_filter(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tblName, filterString), maxParts);
            }

        }

        private void checkLimitNumberOfPartitionsByExpr(String catName, String dbName, String tblName, byte[] filterExpr, int maxParts) throws TException {
            if (this.isPartitionLimitEnabled()) {
                this.checkLimitNumberOfPartitions(tblName, this.get_num_partitions_by_expr(catName, dbName, tblName, filterExpr), maxParts);
            }

        }

        private boolean isPartitionLimitEnabled() {
            int partitionLimit = MetastoreConf.getIntVar(this.conf, ConfVars.LIMIT_PARTITION_REQUEST);
            return partitionLimit > -1;
        }

        private void checkLimitNumberOfPartitions(String tblName, int numPartitions, int maxToFetch) throws MetaException {
            if (this.isPartitionLimitEnabled()) {
                int partitionLimit = MetastoreConf.getIntVar(this.conf, ConfVars.LIMIT_PARTITION_REQUEST);
                int partitionRequest = maxToFetch < 0 ? numPartitions : maxToFetch;
                if (partitionRequest > partitionLimit) {
                    String configName = ConfVars.LIMIT_PARTITION_REQUEST.toString();
                    throw new MetaException(String.format("Number of partitions scanned (=%d) on table '%s' exceeds limit (=%d). This is controlled on the metastore server by %s.", partitionRequest, tblName, partitionLimit, configName));
                }
            }

        }

        public List<PartitionSpec> get_partitions_pspec(String db_name, String tbl_name, int max_parts) throws NoSuchObjectException, MetaException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            String tableName = tbl_name.toLowerCase();
            this.startTableFunction("get_partitions_pspec", parsedDbName[0], parsedDbName[1], tableName);
            List partitionSpecs = null;

            List var13;
            try {
                Table table = this.get_table_core(parsedDbName[0], parsedDbName[1], tableName);
                List<Partition> partitions = this.get_partitions(db_name, tableName, (short)max_parts);
                if (is_partition_spec_grouping_enabled(table)) {
                    partitionSpecs = this.get_partitionspecs_grouped_by_storage_descriptor(table, partitions);
                } else {
                    PartitionSpec pSpec = new PartitionSpec();
                    pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
                    pSpec.setCatName(parsedDbName[0]);
                    pSpec.setDbName(parsedDbName[1]);
                    pSpec.setTableName(tableName);
                    pSpec.setRootPath(table.getSd().getLocation());
                    partitionSpecs = Arrays.asList(pSpec);
                }

                var13 = partitionSpecs;
            } finally {
                this.endFunction("get_partitions_pspec", partitionSpecs != null && !partitionSpecs.isEmpty(), (Exception)null, tbl_name);
            }

            return var13;
        }

        private List<PartitionSpec> get_partitionspecs_grouped_by_storage_descriptor(Table table, List<Partition> partitions) throws NoSuchObjectException, MetaException {
            assert is_partition_spec_grouping_enabled(table);

            final String tablePath = table.getSd().getLocation();
            ImmutableListMultimap<Boolean, Partition> partitionsWithinTableDirectory = Multimaps.index(partitions, new Function<Partition, Boolean>() {
                public Boolean apply(Partition input) {
                    return input.getSd().getLocation().startsWith(tablePath);
                }
            });
            List<PartitionSpec> partSpecs = new ArrayList();
            Map<HiveMetaStore.HMSHandler.StorageDescriptorKey, List<PartitionWithoutSD>> sdToPartList = new HashMap();
            ImmutableList partitionsOutsideTableDir;
            if (partitionsWithinTableDirectory.containsKey(true)) {
                partitionsOutsideTableDir = partitionsWithinTableDirectory.get(true);

                PartitionWithoutSD partitionWithoutSD;
                HiveMetaStore.HMSHandler.StorageDescriptorKey sdKey;
                for(UnmodifiableIterator var8 = partitionsOutsideTableDir.iterator(); var8.hasNext(); ((List)sdToPartList.get(sdKey)).add(partitionWithoutSD)) {
                    Partition partition = (Partition)var8.next();
                    partitionWithoutSD = new PartitionWithoutSD(partition.getValues(), partition.getCreateTime(), partition.getLastAccessTime(), partition.getSd().getLocation().substring(tablePath.length()), partition.getParameters());
                    sdKey = new HiveMetaStore.HMSHandler.StorageDescriptorKey(partition.getSd());
                    if (!sdToPartList.containsKey(sdKey)) {
                        sdToPartList.put(sdKey, new ArrayList());
                    }
                }

                Iterator var12 = sdToPartList.entrySet().iterator();

                while(var12.hasNext()) {
                    Entry<HiveMetaStore.HMSHandler.StorageDescriptorKey, List<PartitionWithoutSD>> entry = (Entry)var12.next();
                    partSpecs.add(this.getSharedSDPartSpec(table, (HiveMetaStore.HMSHandler.StorageDescriptorKey)entry.getKey(), (List)entry.getValue()));
                }
            }

            if (partitionsWithinTableDirectory.containsKey(false)) {
                partitionsOutsideTableDir = partitionsWithinTableDirectory.get(false);
                if (!partitionsOutsideTableDir.isEmpty()) {
                    PartitionSpec partListSpec = new PartitionSpec();
                    partListSpec.setDbName(table.getDbName());
                    partListSpec.setTableName(table.getTableName());
                    partListSpec.setPartitionList(new PartitionListComposingSpec(partitionsOutsideTableDir));
                    partSpecs.add(partListSpec);
                }
            }

            return partSpecs;
        }

        private PartitionSpec getSharedSDPartSpec(Table table, HiveMetaStore.HMSHandler.StorageDescriptorKey sdKey, List<PartitionWithoutSD> partitions) {
            StorageDescriptor sd = new StorageDescriptor(sdKey.getSd());
            sd.setLocation(table.getSd().getLocation());
            PartitionSpecWithSharedSD sharedSDPartSpec = new PartitionSpecWithSharedSD(partitions, sd);
            PartitionSpec ret = new PartitionSpec();
            ret.setRootPath(sd.getLocation());
            ret.setSharedSDPartitionSpec(sharedSDPartSpec);
            ret.setDbName(table.getDbName());
            ret.setTableName(table.getTableName());
            return ret;
        }

        private static boolean is_partition_spec_grouping_enabled(Table table) {
            Map<String, String> parameters = table.getParameters();
            return parameters.containsKey("hive.hcatalog.partition.spec.grouping.enabled") && ((String)parameters.get("hive.hcatalog.partition.spec.grouping.enabled")).equalsIgnoreCase("true");
        }

        public List<String> get_partition_names(String db_name, String tbl_name, short max_parts) throws NoSuchObjectException, MetaException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startTableFunction("get_partition_names", parsedDbName[0], parsedDbName[1], tbl_name);
            this.fireReadTablePreEvent(parsedDbName[0], parsedDbName[1], tbl_name);
            List<String> ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().listPartitionNames(parsedDbName[0], parsedDbName[1], tbl_name, max_parts);
            } catch (Exception var11) {
                ex = var11;
                if (var11 instanceof MetaException) {
                    throw (MetaException)var11;
                }

                throw newMetaException(var11);
            } finally {
                this.endFunction("get_partition_names", ret != null, ex, tbl_name);
            }

            return ret;
        }

        public PartitionValuesResponse get_partition_values(PartitionValuesRequest request) throws MetaException {
            String catName = request.isSetCatName() ? request.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String dbName = request.getDbName();
            String tblName = request.getTblName();
            List<FieldSchema> partCols = new ArrayList();
            partCols.add(request.getPartitionKeys().get(0));
            return this.getMS().listPartitionValues(catName, dbName, tblName, request.getPartitionKeys(), request.isApplyDistinct(), request.getFilter(), request.isAscending(), request.getPartitionOrder(), request.getMaxParts());
        }

        /** @deprecated */
        @Deprecated
        public void alter_partition(String db_name, String tbl_name, Partition new_part) throws TException {
            this.rename_partition(db_name, tbl_name, (List)null, new_part);
        }

        /** @deprecated */
        @Deprecated
        public void alter_partition_with_environment_context(String dbName, String tableName, Partition newPartition, EnvironmentContext envContext) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            this.rename_partition(parsedDbName[0], parsedDbName[1], tableName, (List)null, newPartition, envContext, (String)null);
        }

        /** @deprecated */
        @Deprecated
        public void rename_partition(String db_name, String tbl_name, List<String> part_vals, Partition new_part) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.rename_partition(parsedDbName[0], parsedDbName[1], tbl_name, part_vals, new_part, (EnvironmentContext)null, (String)null);
        }

        public RenamePartitionResponse rename_partition_req(RenamePartitionRequest req) throws InvalidOperationException, MetaException, TException {
            this.rename_partition(req.getCatName(), req.getDbName(), req.getTableName(), req.getPartVals(), req.getNewPart(), (EnvironmentContext)null, req.getValidWriteIdList());
            return new RenamePartitionResponse();
        }

        private void rename_partition(String catName, String db_name, String tbl_name, List<String> part_vals, Partition new_part, EnvironmentContext envContext, String validWriteIds) throws TException {
            this.startTableFunction("alter_partition", catName, db_name, tbl_name);
            if (LOG.isInfoEnabled()) {
                LOG.info("New partition values:" + new_part.getValues());
                if (part_vals != null && part_vals.size() > 0) {
                    LOG.info("Old Partition values:" + part_vals);
                }
            }

            if (new_part.getSd() != null) {
                String newLocation = new_part.getSd().getLocation();
                if (org.apache.commons.lang.StringUtils.isNotEmpty(newLocation)) {
                    Path tblPath = this.wh.getDnsPath(new Path(newLocation));
                    new_part.getSd().setLocation(tblPath.toString());
                }
            }

            if (!new_part.isSetCatName()) {
                new_part.setCatName(catName);
            }

            Partition oldPart = null;
            Object ex = null;

            try {
                this.firePreEvent(new PreAlterPartitionEvent(db_name, tbl_name, part_vals, new_part, this));
                if (part_vals != null && !part_vals.isEmpty()) {
                    MetaStoreUtils.validatePartitionNameCharacters(new_part.getValues(), this.partitionValidationPattern);
                }

                oldPart = this.alterHandler.alterPartition(this.getMS(), this.wh, catName, db_name, tbl_name, part_vals, new_part, envContext, this, validWriteIds);
                Table table = null;
                if (!this.listeners.isEmpty()) {
                    if (table == null) {
                        table = this.getMS().getTable(catName, db_name, tbl_name, (String)null);
                    }

                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALTER_PARTITION, new AlterPartitionEvent(oldPart, new_part, table, false, true, new_part.getWriteId(), this), envContext);
                }
            } catch (InvalidObjectException var16) {
                ex = var16;
                throw new InvalidOperationException(var16.getMessage());
            } catch (AlreadyExistsException var17) {
                ex = var17;
                throw new InvalidOperationException(var17.getMessage());
            } catch (Exception var18) {
                ex = var18;
                if (var18 instanceof MetaException) {
                    throw (MetaException)var18;
                }

                if (var18 instanceof InvalidOperationException) {
                    throw (InvalidOperationException)var18;
                }

                throw newMetaException(var18);
            } finally {
                this.endFunction("alter_partition", oldPart != null, (Exception)ex, tbl_name);
            }

        }

        public void alter_partitions(String db_name, String tbl_name, List<Partition> new_parts) throws TException {
            String[] o = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.alter_partitions_with_environment_context(o[0], o[1], tbl_name, new_parts, (EnvironmentContext)null, (String)null, -1L);
        }

        public AlterPartitionsResponse alter_partitions_req(AlterPartitionsRequest req) throws TException {
            this.alter_partitions_with_environment_context(req.getCatName(), req.getDbName(), req.getTableName(), req.getPartitions(), req.getEnvironmentContext(), req.isSetValidWriteIdList() ? req.getValidWriteIdList() : null, req.isSetWriteId() ? req.getWriteId() : -1L);
            return new AlterPartitionsResponse();
        }

        /** @deprecated */
        @Deprecated
        public void alter_partitions_with_environment_context(String db_name, String tbl_name, List<Partition> new_parts, EnvironmentContext environmentContext) throws TException {
            String[] o = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.alter_partitions_with_environment_context(o[0], o[1], tbl_name, new_parts, environmentContext, (String)null, -1L);
        }

        private void alter_partitions_with_environment_context(String catName, String db_name, String tbl_name, List<Partition> new_parts, EnvironmentContext environmentContext, String writeIdList, long writeId) throws TException {

            String[] parsedDbName = parseDbName(db_name, conf);
            startTableFunction("alter_partitions", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);

            if (LOG.isInfoEnabled()) {
                for (Partition tmpPart : new_parts) {
                    LOG.info("New partition values:" + tmpPart.getValues());
                }
            }
            // all partitions are altered atomically
            // all prehooks are fired together followed by all post hooks
            List<Partition> oldParts = null;
            Exception ex = null;
            try {
                for (Partition tmpPart : new_parts) {
                    // Make sure the catalog name is set in the new partition
                    if (!tmpPart.isSetCatName()) {
                        tmpPart.setCatName(getDefaultCatalog(conf));
                    }
                    firePreEvent(new PreAlterPartitionEvent(parsedDbName[DB_NAME], tbl_name, null, tmpPart, this));
                }
                oldParts = alterHandler.alterPartitions(this.getMS(), this.wh, catName, db_name, tbl_name, new_parts, environmentContext, writeIdList, writeId, this);;
                Iterator<Partition> olditr = oldParts.iterator();
                // Only fetch the table if we have a listener that needs it.
                Table table = null;
                for (Partition tmpPart : new_parts) {
                    Partition oldTmpPart;
                    if (olditr.hasNext()) {
                        oldTmpPart = olditr.next();
                    }
                    else {
                        throw new InvalidOperationException("failed to alterpartitions");
                    }

                    if (table == null) {
                        table = getMS().getTable(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
                    }

                    if (!listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALTER_PARTITION, new AlterPartitionEvent(oldTmpPart, tmpPart, table, false, true, writeId, this));
                    }
                }
            } catch (InvalidObjectException e) {
                ex = e;
                throw new InvalidOperationException(e.getMessage());
            } catch (AlreadyExistsException e) {
                ex = e;
                throw new InvalidOperationException(e.getMessage());
            } catch (Exception e) {
                ex = e;
                if (e instanceof MetaException) {
                    throw (MetaException) e;
                } else if (e instanceof InvalidOperationException) {
                    throw (InvalidOperationException) e;
                } else {
                    throw newMetaException(e);
                }
            } finally {
                endFunction("alter_partition", oldParts != null, ex, tbl_name);
            }
        }

        public String getVersion() throws TException {
            this.endFunction(this.startFunction("getVersion"), true, (Exception)null);
            return "3.0";
        }

        public void alter_table(String dbname, String name, Table newTable) throws InvalidOperationException, MetaException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbname, this.conf);
            this.alter_table_core(parsedDbName[0], parsedDbName[1], name, newTable, (EnvironmentContext)null, (String)null);
        }

        public void alter_table_with_cascade(String dbname, String name, Table newTable, boolean cascade) throws InvalidOperationException, MetaException {
            EnvironmentContext envContext = null;
            if (cascade) {
                envContext = new EnvironmentContext();
                envContext.putToProperties("CASCADE", "true");
            }

            String[] parsedDbName = MetaStoreUtils.parseDbName(dbname, this.conf);
            this.alter_table_core(parsedDbName[0], parsedDbName[1], name, newTable, envContext, (String)null);
        }

        public AlterTableResponse alter_table_req(AlterTableRequest req) throws InvalidOperationException, MetaException, TException {
            this.alter_table_core(req.getCatName(), req.getDbName(), req.getTableName(), req.getTable(), req.getEnvironmentContext(), req.getValidWriteIdList());
            return new AlterTableResponse();
        }

        public void alter_table_with_environment_context(String dbname, String name, Table newTable, EnvironmentContext envContext) throws InvalidOperationException, MetaException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbname, this.conf);
            this.alter_table_core(parsedDbName[0], parsedDbName[1], name, newTable, envContext, (String)null);
        }

        private void alter_table_core(String catName, String dbname, String name, Table newTable, EnvironmentContext envContext, String validWriteIdList) throws InvalidOperationException, MetaException {
            this.startFunction("alter_table", ": " + Warehouse.getCatalogQualifiedTableName(catName, dbname, name) + " newtbl=" + newTable.getTableName());
            if (envContext == null) {
                envContext = new EnvironmentContext();
            }

            if (catName == null) {
                catName = MetaStoreUtils.getDefaultCatalog(this.conf);
            }

            if (newTable.getParameters() == null || newTable.getParameters().get("transient_lastDdlTime") == null) {
                newTable.putToParameters("transient_lastDdlTime", Long.toString(System.currentTimeMillis() / 1000L));
            }

            if (newTable.getSd() != null) {
                String newLocation = newTable.getSd().getLocation();
                if (org.apache.commons.lang.StringUtils.isNotEmpty(newLocation)) {
                    Path tblPath = this.wh.getDnsPath(new Path(newLocation));
                    newTable.getSd().setLocation(tblPath.toString());
                }
            }

            if (!newTable.isSetCatName()) {
                newTable.setCatName(catName);
            }

            boolean success = false;
            Object ex = null;

            try {
                Table oldt = this.get_table_core(catName, dbname, name);
                this.firePreEvent(new PreAlterTableEvent(oldt, newTable, this));
                this.alterHandler.alterTable(this.getMS(), this.wh, catName, dbname, name, newTable, envContext, this, validWriteIdList);
                success = true;
            } catch (NoSuchObjectException var14) {
                ex = var14;
                throw new InvalidOperationException(var14.getMessage());
            } catch (Exception var15) {
                ex = var15;
                if (var15 instanceof MetaException) {
                    throw (MetaException)var15;
                }

                if (var15 instanceof InvalidOperationException) {
                    throw (InvalidOperationException)var15;
                }

                throw newMetaException(var15);
            } finally {
                this.endFunction("alter_table", success, (Exception)ex, name);
            }

        }

        public List<String> get_tables(String dbname, String pattern) throws MetaException {
            this.startFunction("get_tables", ": db=" + dbname + " pat=" + pattern);
            List<String> ret = null;
            Exception ex = null;
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbname, this.conf);

            try {
                ret = this.getMS().getTables(parsedDbName[0], parsedDbName[1], pattern);
            } catch (Exception var10) {
                ex = var10;
                if (var10 instanceof MetaException) {
                    throw (MetaException)var10;
                }

                throw newMetaException(var10);
            } finally {
                this.endFunction("get_tables", ret != null, ex);
            }

            return ret;
        }

        public List<String> get_tables_by_type(String dbname, String pattern, String tableType) throws MetaException {
            this.startFunction("get_tables_by_type", ": db=" + dbname + " pat=" + pattern + ",type=" + tableType);
            List<String> ret = null;
            Exception ex = null;
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbname, this.conf);

            try {
                ret = this.getMS().getTables(parsedDbName[0], parsedDbName[1], pattern, TableType.valueOf(tableType));
            } catch (Exception var11) {
                ex = var11;
                if (var11 instanceof MetaException) {
                    throw (MetaException)var11;
                }

                throw newMetaException(var11);
            } finally {
                this.endFunction("get_tables_by_type", ret != null, ex);
            }

            return ret;
        }

        public List<Table> get_all_materialized_view_objects_for_rewriting() throws MetaException {
            this.startFunction("get_all_materialized_view_objects_for_rewriting");
            List<Table> ret = null;
            Object ex = null;

            try {
                ret = this.getMS().getAllMaterializedViewObjectsForRewriting("hive");
            } catch (MetaException var8) {
                ex = var8;
                throw var8;
            } catch (Exception var9) {
                ex = var9;
                throw newMetaException(var9);
            } finally {
                this.endFunction("get_all_materialized_view_objects_for_rewriting", ret != null, (Exception)ex);
            }

            return ret;
        }

        public List<String> get_materialized_views_for_rewriting(String dbname) throws MetaException {
            this.startFunction("get_materialized_views_for_rewriting", ": db=" + dbname);
            List<String> ret = null;
            Exception ex = null;
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbname, this.conf);

            try {
                ret = this.getMS().getMaterializedViewsForRewriting(parsedDbName[0], parsedDbName[1]);
            } catch (Exception var9) {
                ex = var9;
                if (var9 instanceof MetaException) {
                    throw (MetaException)var9;
                }

                throw newMetaException(var9);
            } finally {
                this.endFunction("get_materialized_views_for_rewriting", ret != null, ex);
            }

            return ret;
        }

        public List<String> get_all_tables(String dbname) throws MetaException {
            this.startFunction("get_all_tables", ": db=" + dbname);
            List<String> ret = null;
            Exception ex = null;
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbname, this.conf);

            try {
                ret = this.getMS().getAllTables(parsedDbName[0], parsedDbName[1]);
            } catch (Exception var9) {
                ex = var9;
                if (var9 instanceof MetaException) {
                    throw (MetaException)var9;
                }

                throw newMetaException(var9);
            } finally {
                this.endFunction("get_all_tables", ret != null, ex);
            }

            return ret;
        }

        public List<FieldSchema> get_fields(String db, String tableName) throws MetaException, UnknownTableException, UnknownDBException {
            return this.get_fields_with_environment_context(db, tableName, (EnvironmentContext)null);
        }

        public List<FieldSchema> get_fields_with_environment_context(String db, String tableName, EnvironmentContext envContext) throws MetaException, UnknownTableException, UnknownDBException {
            this.startFunction("get_fields_with_environment_context", ": db=" + db + "tbl=" + tableName);
            String[] names = tableName.split("\\.");
            String base_table_name = names[0];
            String[] parsedDbName = MetaStoreUtils.parseDbName(db, this.conf);
            List<FieldSchema> ret = null;
            Exception ex = null;
            Object orgHiveLoader = null;

            try {
                Table tbl;
                try {
                    tbl = this.get_table_core(parsedDbName[0], parsedDbName[1], base_table_name);
                } catch (NoSuchObjectException var16) {
                    throw new UnknownTableException(var16.getMessage());
                }

                if (null != tbl.getSd().getSerdeInfo().getSerializationLib() && !MetastoreConf.getStringCollection(this.conf, ConfVars.SERDES_USING_METASTORE_FOR_SCHEMA).contains(tbl.getSd().getSerdeInfo().getSerializationLib())) {
                    StorageSchemaReader schemaReader = this.getStorageSchemaReader();
                    ret = schemaReader.readSchema(tbl, envContext, this.getConf());
                } else {
                    ret = tbl.getSd().getCols();
                }
            } catch (Exception var17) {
                ex = var17;
                if (var17 instanceof UnknownTableException) {
                    throw (UnknownTableException)var17;
                }

                if (var17 instanceof MetaException) {
                    throw (MetaException)var17;
                }

                throw newMetaException(var17);
            } finally {
                if (orgHiveLoader != null) {
                    this.conf.setClassLoader((ClassLoader)orgHiveLoader);
                }

                this.endFunction("get_fields_with_environment_context", ret != null, ex, tableName);
            }

            return ret;
        }

        private StorageSchemaReader getStorageSchemaReader() throws MetaException {
            if (this.storageSchemaReader == null) {
                String className = MetastoreConf.getVar(this.conf, ConfVars.STORAGE_SCHEMA_READER_IMPL);
                Class readerClass = JavaUtils.getClass(className, StorageSchemaReader.class);

                try {
                    this.storageSchemaReader = (StorageSchemaReader)readerClass.newInstance();
                } catch (IllegalAccessException | InstantiationException var4) {
                    LOG.error("Unable to instantiate class " + className, var4);
                    throw new MetaException(var4.getMessage());
                }
            }

            return this.storageSchemaReader;
        }

        public List<FieldSchema> get_schema(String db, String tableName) throws MetaException, UnknownTableException, UnknownDBException {
            return this.get_schema_with_environment_context(db, tableName, (EnvironmentContext)null);
        }

        public List<FieldSchema> get_schema_with_environment_context(String db, String tableName, EnvironmentContext envContext) throws MetaException, UnknownTableException, UnknownDBException {
            this.startFunction("get_schema_with_environment_context", ": db=" + db + "tbl=" + tableName);
            boolean success = false;
            Exception ex = null;

            List var11;
            try {
                String[] names = tableName.split("\\.");
                String base_table_name = names[0];
                String[] parsedDbName = MetaStoreUtils.parseDbName(db, this.conf);

                Table tbl;
                try {
                    tbl = this.get_table_core(parsedDbName[0], parsedDbName[1], base_table_name);
                } catch (NoSuchObjectException var16) {
                    throw new UnknownTableException(var16.getMessage());
                }

                List<FieldSchema> fieldSchemas = this.get_fields_with_environment_context(db, base_table_name, envContext);
                if (tbl == null || fieldSchemas == null) {
                    throw new UnknownTableException(tableName + " doesn't exist");
                }

                if (tbl.getPartitionKeys() != null) {
                    fieldSchemas.addAll(tbl.getPartitionKeys());
                }

                success = true;
                var11 = fieldSchemas;
            } catch (Exception var17) {
                ex = var17;
                if (var17 instanceof UnknownDBException) {
                    throw (UnknownDBException)var17;
                }

                if (var17 instanceof UnknownTableException) {
                    throw (UnknownTableException)var17;
                }

                if (var17 instanceof MetaException) {
                    throw (MetaException)var17;
                }

                MetaException me = new MetaException(var17.toString());
                me.initCause(var17);
                throw me;
            } finally {
                this.endFunction("get_schema_with_environment_context", success, ex, tableName);
            }

            return var11;
        }

        public String getCpuProfile(int profileDurationInSec) throws TException {
            return "";
        }

        public String get_config_value(String name, String defaultValue) throws TException {
            this.startFunction("get_config_value", ": name=" + name + " defaultValue=" + defaultValue);
            boolean success = false;
            Exception ex = null;

            String toReturn;
            try {
                if (name != null) {
                    if (!Pattern.matches("(hive|hdfs|mapred|metastore).*", name)) {
                        throw new ConfigValSecurityException("For security reasons, the config key " + name + " cannot be accessed");
                    }

                    toReturn = defaultValue;

                    try {
                        toReturn = MetastoreConf.get(this.conf, name);
                        if (toReturn == null) {
                            toReturn = defaultValue;
                        }
                    } catch (RuntimeException var11) {
                        LOG.error(((Integer)threadLocalId.get()).toString() + ": " + "RuntimeException thrown in get_config_value - msg: " + var11.getMessage() + " cause: " + var11.getCause());
                    }

                    success = true;
                    String var6 = toReturn;
                    return var6;
                }

                success = true;
                toReturn = defaultValue;
            } catch (Exception var12) {
                ex = var12;
                if (var12 instanceof ConfigValSecurityException) {
                    throw (ConfigValSecurityException)var12;
                }

                throw new TException(var12);
            } finally {
                this.endFunction("get_config_value", success, ex);
            }

            return toReturn;
        }

        private List<String> getPartValsFromName(Table t, String partName) throws MetaException, InvalidObjectException {
            Preconditions.checkArgument(t != null, "Table can not be null");
            LinkedHashMap<String, String> hm = Warehouse.makeSpecFromName(partName);
            List<String> partVals = new ArrayList();
            Iterator var5 = t.getPartitionKeys().iterator();

            while(var5.hasNext()) {
                FieldSchema field = (FieldSchema)var5.next();
                String key = field.getName();
                String val = (String)hm.get(key);
                if (val == null) {
                    throw new InvalidObjectException("incomplete partition name - missing " + key);
                }

                partVals.add(val);
            }

            return partVals;
        }

        private List<String> getPartValsFromName(RawStore ms, String catName, String dbName, String tblName, String partName) throws MetaException, InvalidObjectException {
            Table t = ms.getTable(catName, dbName, tblName, (String)null);
            if (t == null) {
                throw new InvalidObjectException(dbName + "." + tblName + " table not found");
            } else {
                return this.getPartValsFromName(t, partName);
            }
        }

        private Partition get_partition_by_name_core(RawStore ms, String catName, String db_name, String tbl_name, String part_name) throws TException {
            this.fireReadTablePreEvent(catName, db_name, tbl_name);

            List partVals;
            try {
                partVals = this.getPartValsFromName(ms, catName, db_name, tbl_name, part_name);
            } catch (InvalidObjectException var8) {
                throw new NoSuchObjectException(var8.getMessage());
            }

            Partition p = ms.getPartition(catName, db_name, tbl_name, partVals);
            if (p == null) {
                throw new NoSuchObjectException(Warehouse.getCatalogQualifiedTableName(catName, db_name, tbl_name) + " partition (" + part_name + ") not found");
            } else {
                return p;
            }
        }

        public Partition get_partition_by_name(String db_name, String tbl_name, String part_name) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startFunction("get_partition_by_name", ": tbl=" + Warehouse.getCatalogQualifiedTableName(parsedDbName[0], parsedDbName[1], tbl_name) + " part=" + part_name);
            Partition ret = null;
            Exception ex = null;

            try {
                ret = this.get_partition_by_name_core(this.getMS(), parsedDbName[0], parsedDbName[1], tbl_name, part_name);
            } catch (Exception var11) {
                ex = var11;
                this.rethrowException(var11);
            } finally {
                this.endFunction("get_partition_by_name", ret != null, ex, tbl_name);
            }

            return ret;
        }

        public Partition append_partition_by_name(String db_name, String tbl_name, String part_name) throws TException {
            return this.append_partition_by_name_with_environment_context(db_name, tbl_name, part_name, (EnvironmentContext)null);
        }

        public Partition append_partition_by_name_with_environment_context(String db_name, String tbl_name, String part_name, EnvironmentContext env_context) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startFunction("append_partition_by_name", ": tbl=" + Warehouse.getCatalogQualifiedTableName(parsedDbName[0], parsedDbName[1], tbl_name) + " part=" + part_name);
            Partition ret = null;
            Exception ex = null;

            try {
                RawStore ms = this.getMS();
                List<String> partVals = this.getPartValsFromName(ms, parsedDbName[0], parsedDbName[1], tbl_name, part_name);
                ret = this.append_partition_common(ms, parsedDbName[0], parsedDbName[1], tbl_name, partVals, env_context);
            } catch (Exception var13) {
                ex = var13;
                if (var13 instanceof InvalidObjectException) {
                    throw (InvalidObjectException)var13;
                }

                if (var13 instanceof AlreadyExistsException) {
                    throw (AlreadyExistsException)var13;
                }

                if (var13 instanceof MetaException) {
                    throw (MetaException)var13;
                }

                throw newMetaException(var13);
            } finally {
                this.endFunction("append_partition_by_name", ret != null, ex, tbl_name);
            }

            return ret;
        }

        private boolean drop_partition_by_name_core(RawStore ms, String catName, String db_name, String tbl_name, String part_name, boolean deleteData, EnvironmentContext envContext) throws TException, IOException {
            List partVals;
            try {
                partVals = this.getPartValsFromName(ms, catName, db_name, tbl_name, part_name);
            } catch (InvalidObjectException var10) {
                throw new NoSuchObjectException(var10.getMessage());
            }

            return this.drop_partition_common(ms, catName, db_name, tbl_name, partVals, deleteData, envContext);
        }

        public boolean drop_partition_by_name(String db_name, String tbl_name, String part_name, boolean deleteData) throws TException {
            return this.drop_partition_by_name_with_environment_context(db_name, tbl_name, part_name, deleteData, (EnvironmentContext)null);
        }

        public boolean drop_partition_by_name_with_environment_context(String db_name, String tbl_name, String part_name, boolean deleteData, EnvironmentContext envContext) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startFunction("drop_partition_by_name", ": tbl=" + Warehouse.getCatalogQualifiedTableName(parsedDbName[0], parsedDbName[1], tbl_name) + " part=" + part_name);
            boolean ret = false;
            Object ex = null;

            try {
                ret = this.drop_partition_by_name_core(this.getMS(), parsedDbName[0], parsedDbName[1], tbl_name, part_name, deleteData, envContext);
            } catch (IOException var14) {
                ex = var14;
                throw new MetaException(var14.getMessage());
            } catch (Exception var15) {
                ex = var15;
                this.rethrowException(var15);
            } finally {
                this.endFunction("drop_partition_by_name", ret, (Exception)ex, tbl_name);
            }

            return ret;
        }

        public List<Partition> get_partitions_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startPartitionFunction("get_partitions_ps", parsedDbName[0], parsedDbName[1], tbl_name, part_vals);
            List<Partition> ret = null;
            Exception ex = null;

            try {
                ret = this.get_partitions_ps_with_auth(db_name, tbl_name, part_vals, max_parts, (String)null, (List)null);
            } catch (Exception var12) {
                ex = var12;
                this.rethrowException(var12);
            } finally {
                this.endFunction("get_partitions_ps", ret != null, ex, tbl_name);
            }

            return ret;
        }

        public List<Partition> get_partitions_ps_with_auth(String db_name, String tbl_name, List<String> part_vals, short max_parts, String userName, List<String> groupNames) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startPartitionFunction("get_partitions_ps_with_auth", parsedDbName[0], parsedDbName[1], tbl_name, part_vals);
            this.fireReadTablePreEvent(parsedDbName[0], parsedDbName[1], tbl_name);
            List<Partition> ret = null;
            Object ex = null;

            try {
                ret = this.getMS().listPartitionsPsWithAuth(parsedDbName[0], parsedDbName[1], tbl_name, part_vals, max_parts, userName, groupNames);
            } catch (InvalidObjectException var15) {
                ex = var15;
                throw new MetaException(var15.getMessage());
            } catch (Exception var16) {
                ex = var16;
                this.rethrowException(var16);
            } finally {
                this.endFunction("get_partitions_ps_with_auth", ret != null, (Exception)ex, tbl_name);
            }

            return ret;
        }

        public List<String> get_partition_names_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startPartitionFunction("get_partitions_names_ps", parsedDbName[0], parsedDbName[1], tbl_name, part_vals);
            this.fireReadTablePreEvent(parsedDbName[0], parsedDbName[1], tbl_name);
            List<String> ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().listPartitionNamesPs(parsedDbName[0], parsedDbName[1], tbl_name, part_vals, max_parts);
            } catch (Exception var12) {
                ex = var12;
                this.rethrowException(var12);
            } finally {
                this.endFunction("get_partitions_names_ps", ret != null, ex, tbl_name);
            }

            return ret;
        }

        public List<String> partition_name_to_vals(String part_name) throws TException {
            if (part_name.length() == 0) {
                return new ArrayList();
            } else {
                LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(part_name);
                List<String> part_vals = new ArrayList();
                part_vals.addAll(map.values());
                return part_vals;
            }
        }

        public Map<String, String> partition_name_to_spec(String part_name) throws TException {
            return (Map)(part_name.length() == 0 ? new HashMap() : Warehouse.makeSpecFromName(part_name));
        }

        private String lowerCaseConvertPartName(String partName) throws MetaException {
            if (partName == null) {
                return partName;
            } else {
                boolean isFirst = true;
                Map<String, String> partSpec = Warehouse.makeEscSpecFromName(partName);
                String convertedPartName = new String();

                String partColName;
                String partColVal;
                for(Iterator var5 = partSpec.entrySet().iterator(); var5.hasNext(); convertedPartName = convertedPartName + partColName.toLowerCase() + "=" + partColVal) {
                    Entry<String, String> entry = (Entry)var5.next();
                    partColName = (String)entry.getKey();
                    partColVal = (String)entry.getValue();
                    if (!isFirst) {
                        convertedPartName = convertedPartName + "/";
                    } else {
                        isFirst = false;
                    }
                }

                return convertedPartName;
            }
        }

        /** @deprecated */
        @Deprecated
        public ColumnStatistics get_table_column_statistics(String dbName, String tableName, String colName) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            parsedDbName[0] = parsedDbName[0].toLowerCase();
            parsedDbName[1] = parsedDbName[1].toLowerCase();
            tableName = tableName.toLowerCase();
            colName = colName.toLowerCase();
            this.startFunction("get_column_statistics_by_table", ": table=" + Warehouse.getCatalogQualifiedTableName(parsedDbName[0], parsedDbName[1], tableName) + " column=" + colName);
            ColumnStatistics statsObj = null;

            ColumnStatistics var6;
            try {
                statsObj = this.getMS().getTableColumnStatistics(parsedDbName[0], parsedDbName[1], tableName, Lists.newArrayList(new String[]{colName}), (String)null);

                assert statsObj == null || statsObj.getStatsObjSize() <= 1;

                var6 = statsObj;
            } finally {
                this.endFunction("get_column_statistics_by_table", statsObj != null, (Exception)null, tableName);
            }

            return var6;
        }

        public TableStatsResult get_table_statistics_req(TableStatsRequest request) throws TException {
            String catName = request.isSetCatName() ? request.getCatName().toLowerCase() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String dbName = request.getDbName().toLowerCase();
            String tblName = request.getTblName().toLowerCase();
            this.startFunction("get_table_statistics_req", ": table=" + Warehouse.getCatalogQualifiedTableName(catName, dbName, tblName));
            TableStatsResult result = null;
            List<String> lowerCaseColNames = new ArrayList(request.getColNames().size());
            Iterator var7 = request.getColNames().iterator();

            while(var7.hasNext()) {
                String colName = (String)var7.next();
                lowerCaseColNames.add(colName.toLowerCase());
            }

            try {
                ColumnStatistics cs = this.getMS().getTableColumnStatistics(catName, dbName, tblName, lowerCaseColNames, request.getValidWriteIdList());
                result = new TableStatsResult((List)(cs != null && cs.getStatsObj() != null && (!cs.isSetIsStatsCompliant() || cs.isIsStatsCompliant()) ? cs.getStatsObj() : Lists.newArrayList()));
            } finally {
                this.endFunction("get_table_statistics_req", result == null, (Exception)null, tblName);
            }

            return result;
        }

        /** @deprecated */
        @Deprecated
        public ColumnStatistics get_partition_column_statistics(String dbName, String tableName, String partName, String colName) throws TException {
            dbName = dbName.toLowerCase();
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            tableName = tableName.toLowerCase();
            colName = colName.toLowerCase();
            String convertedPartName = this.lowerCaseConvertPartName(partName);
            this.startFunction("get_column_statistics_by_partition", ": table=" + Warehouse.getCatalogQualifiedTableName(parsedDbName[0], parsedDbName[1], tableName) + " partition=" + convertedPartName + " column=" + colName);
            ColumnStatistics statsObj = null;

            try {
                List<ColumnStatistics> list = this.getMS().getPartitionColumnStatistics(parsedDbName[0], parsedDbName[1], tableName, Lists.newArrayList(new String[]{convertedPartName}), Lists.newArrayList(new String[]{colName}));
                if (list.isEmpty()) {
                    Object var9 = null;
                    return (ColumnStatistics)var9;
                }

                if (list.size() != 1) {
                    throw new MetaException(list.size() + " statistics for single column and partition");
                }

                statsObj = (ColumnStatistics)list.get(0);
            } finally {
                this.endFunction("get_column_statistics_by_partition", statsObj != null, (Exception)null, tableName);
            }

            return statsObj;
        }

        public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest request) throws TException {
            String catName = request.isSetCatName() ? request.getCatName().toLowerCase() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String dbName = request.getDbName().toLowerCase();
            String tblName = request.getTblName().toLowerCase();
            this.startFunction("get_partitions_statistics_req", ": table=" + Warehouse.getCatalogQualifiedTableName(catName, dbName, tblName));
            PartitionsStatsResult result = null;
            List<String> lowerCaseColNames = new ArrayList(request.getColNames().size());
            Iterator var7 = request.getColNames().iterator();

            while(var7.hasNext()) {
                String colName = (String)var7.next();
                lowerCaseColNames.add(colName.toLowerCase());
            }

            List<String> lowerCasePartNames = new ArrayList(request.getPartNames().size());
            Iterator var16 = request.getPartNames().iterator();

            while(var16.hasNext()) {
                String partName = (String)var16.next();
                lowerCasePartNames.add(this.lowerCaseConvertPartName(partName));
            }

            try {
                List<ColumnStatistics> stats = this.getMS().getPartitionColumnStatistics(catName, dbName, tblName, lowerCasePartNames, lowerCaseColNames, request.isSetValidWriteIdList() ? request.getValidWriteIdList() : null);
                Map<String, List<ColumnStatisticsObj>> map = new HashMap();
                if (stats != null) {
                    Iterator var10 = stats.iterator();

                    label123:
                    while(true) {
                        ColumnStatistics stat;
                        do {
                            if (!var10.hasNext()) {
                                break label123;
                            }

                            stat = (ColumnStatistics)var10.next();
                        } while(stat.isSetIsStatsCompliant() && !stat.isIsStatsCompliant());

                        map.put(stat.getStatsDesc().getPartName(), stat.getStatsObj());
                    }
                }

                result = new PartitionsStatsResult(map);
            } finally {
                this.endFunction("get_partitions_statistics_req", result == null, (Exception)null, tblName);
            }

            return result;
        }

        public boolean update_table_column_statistics(ColumnStatistics colStats) throws TException {
            return this.updateTableColumnStatsInternal(colStats, (String)null, -1L);
        }

        public SetPartitionsStatsResponse update_table_column_statistics_req(SetPartitionsStatsRequest req) throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
            if (req.getColStatsSize() != 1) {
                throw new InvalidInputException("Only one stats object expected");
            } else if (req.isNeedMerge()) {
                throw new InvalidInputException("Merge is not supported for non-aggregate stats");
            } else {
                ColumnStatistics colStats = (ColumnStatistics)req.getColStatsIterator().next();
                boolean ret = this.updateTableColumnStatsInternal(colStats, req.getValidWriteIdList(), req.getWriteId());
                return new SetPartitionsStatsResponse(ret);
            }
        }

        private boolean updateTableColumnStatsInternal(ColumnStatistics colStats, String validWriteIds, long writeId) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
            this.normalizeColStatsInput(colStats);
            this.startFunction("write_column_statistics", ":  table=" + colStats.getStatsDesc().getCatName() + "." + colStats.getStatsDesc().getDbName() + "." + colStats.getStatsDesc().getTableName());
            Map<String, String> parameters = null;
            this.getMS().openTransaction();
            boolean committed = false;

            try {
                parameters = this.getMS().updateTableColumnStatistics(colStats, validWriteIds, writeId);
                if (parameters != null) {
                    Table tableObj = this.getMS().getTable(colStats.getStatsDesc().getCatName(), colStats.getStatsDesc().getDbName(), colStats.getStatsDesc().getTableName(), validWriteIds);
                    if (this.transactionalListeners != null && !this.transactionalListeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.UPDATE_TABLE_COLUMN_STAT, new UpdateTableColumnStatEvent(colStats, tableObj, parameters, writeId, this));
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.UPDATE_TABLE_COLUMN_STAT, new UpdateTableColumnStatEvent(colStats, tableObj, parameters, writeId, this));
                    }
                }

                committed = this.getMS().commitTransaction();
            } finally {
                if (!committed) {
                    this.getMS().rollbackTransaction();
                }

                this.endFunction("write_column_statistics", parameters != null, (Exception)null, colStats.getStatsDesc().getTableName());
            }

            return parameters != null;
        }

        private void normalizeColStatsInput(ColumnStatistics colStats) throws MetaException {
            ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
            statsDesc.setCatName(statsDesc.isSetCatName() ? statsDesc.getCatName().toLowerCase() : MetaStoreUtils.getDefaultCatalog(this.conf));
            statsDesc.setDbName(statsDesc.getDbName().toLowerCase());
            statsDesc.setTableName(statsDesc.getTableName().toLowerCase());
            statsDesc.setPartName(this.lowerCaseConvertPartName(statsDesc.getPartName()));
            long time = System.currentTimeMillis() / 1000L;
            statsDesc.setLastAnalyzed(time);
            Iterator var5 = colStats.getStatsObj().iterator();

            while(var5.hasNext()) {
                ColumnStatisticsObj statsObj = (ColumnStatisticsObj)var5.next();
                statsObj.setColName(statsObj.getColName().toLowerCase());
                statsObj.setColType(statsObj.getColType().toLowerCase());
            }

            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(colStats.getStatsObj());
        }

        private boolean updatePartitonColStatsInternal(Table tbl, ColumnStatistics colStats, String validWriteIds, long writeId) throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
            this.normalizeColStatsInput(colStats);
            ColumnStatisticsDesc csd = colStats.getStatsDesc();
            String catName = csd.getCatName();
            String dbName = csd.getDbName();
            String tableName = csd.getTableName();
            this.startFunction("write_partition_column_statistics", ":  db=" + dbName + " table=" + tableName + " part=" + csd.getPartName());
            boolean ret = false;
            boolean committed = false;
            this.getMS().openTransaction();

            Map parameters;
            try {
                if (tbl == null) {
                    tbl = this.getTable(catName, dbName, tableName);
                }

                List<String> partVals = this.getPartValsFromName(tbl, csd.getPartName());
                parameters = this.getMS().updatePartitionColumnStatistics(colStats, partVals, validWriteIds, writeId);
                if (parameters != null) {
                    if (this.transactionalListeners != null && !this.transactionalListeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.UPDATE_PARTITION_COLUMN_STAT, new UpdatePartitionColumnStatEvent(colStats, partVals, parameters, tbl, writeId, this));
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.UPDATE_PARTITION_COLUMN_STAT, new UpdatePartitionColumnStatEvent(colStats, partVals, parameters, tbl, writeId, this));
                    }
                }

                committed = this.getMS().commitTransaction();
            } finally {
                if (!committed) {
                    this.getMS().rollbackTransaction();
                }

                this.endFunction("write_partition_column_statistics", ret, (Exception)null, tableName);
            }

            return parameters != null;
        }

        public boolean update_partition_column_statistics(ColumnStatistics colStats) throws TException {
            return this.updatePartitonColStatsInternal((Table)null, colStats, (String)null, -1L);
        }

        public SetPartitionsStatsResponse update_partition_column_statistics_req(SetPartitionsStatsRequest req) throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
            if (req.getColStatsSize() != 1) {
                throw new InvalidInputException("Only one stats object expected");
            } else if (req.isNeedMerge()) {
                throw new InvalidInputException("Merge is not supported for non-aggregate stats");
            } else {
                ColumnStatistics colStats = (ColumnStatistics)req.getColStatsIterator().next();
                boolean ret = this.updatePartitonColStatsInternal((Table)null, colStats, req.getValidWriteIdList(), req.getWriteId());
                return new SetPartitionsStatsResponse(ret);
            }
        }

        public boolean delete_partition_column_statistics(String dbName, String tableName, String partName, String colName) throws TException {
            dbName = dbName.toLowerCase();
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            tableName = tableName.toLowerCase();
            if (colName != null) {
                colName = colName.toLowerCase();
            }

            String convertedPartName = this.lowerCaseConvertPartName(partName);
            this.startFunction("delete_column_statistics_by_partition", ": table=" + Warehouse.getCatalogQualifiedTableName(parsedDbName[0], parsedDbName[1], tableName) + " partition=" + convertedPartName + " column=" + colName);
            boolean ret = false;
            boolean committed = false;
            this.getMS().openTransaction();

            try {
                List<String> partVals = this.getPartValsFromName(this.getMS(), parsedDbName[0], parsedDbName[1], tableName, convertedPartName);
                Table table = this.getMS().getTable(parsedDbName[0], parsedDbName[1], tableName);
                if (TxnUtils.isTransactionalTable(table)) {
                    throw new MetaException("Cannot delete stats via this API for a transactional table");
                }

                ret = this.getMS().deletePartitionColumnStatistics(parsedDbName[0], parsedDbName[1], tableName, convertedPartName, partVals, colName);
                if (ret) {
                    if (this.transactionalListeners != null && !this.transactionalListeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.DELETE_PARTITION_COLUMN_STAT, new DeletePartitionColumnStatEvent(parsedDbName[0], parsedDbName[1], tableName, convertedPartName, partVals, colName, this));
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.DELETE_PARTITION_COLUMN_STAT, new DeletePartitionColumnStatEvent(parsedDbName[0], parsedDbName[1], tableName, convertedPartName, partVals, colName, this));
                    }
                }

                committed = this.getMS().commitTransaction();
            } finally {
                if (!committed) {
                    this.getMS().rollbackTransaction();
                }

                this.endFunction("delete_column_statistics_by_partition", ret, (Exception)null, tableName);
            }

            return ret;
        }

        public boolean delete_table_column_statistics(String dbName, String tableName, String colName) throws TException {
            dbName = dbName.toLowerCase();
            tableName = tableName.toLowerCase();
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            if (colName != null) {
                colName = colName.toLowerCase();
            }

            this.startFunction("delete_column_statistics_by_table", ": table=" + Warehouse.getCatalogQualifiedTableName(parsedDbName[0], parsedDbName[1], tableName) + " column=" + colName);
            boolean ret = false;
            boolean committed = false;
            this.getMS().openTransaction();

            try {
                Table table = this.getMS().getTable(parsedDbName[0], parsedDbName[1], tableName);
                if (TxnUtils.isTransactionalTable(table)) {
                    throw new MetaException("Cannot delete stats via this API for a transactional table");
                }

                ret = this.getMS().deleteTableColumnStatistics(parsedDbName[0], parsedDbName[1], tableName, colName);
                if (ret) {
                    if (this.transactionalListeners != null && !this.transactionalListeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.DELETE_TABLE_COLUMN_STAT, new DeleteTableColumnStatEvent(parsedDbName[0], parsedDbName[1], tableName, colName, this));
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.DELETE_TABLE_COLUMN_STAT, new DeleteTableColumnStatEvent(parsedDbName[0], parsedDbName[1], tableName, colName, this));
                    }
                }

                committed = this.getMS().commitTransaction();
            } finally {
                if (!committed) {
                    this.getMS().rollbackTransaction();
                }

                this.endFunction("delete_column_statistics_by_table", ret, (Exception)null, tableName);
            }

            return ret;
        }

        public List<Partition> get_partitions_by_filter(String dbName, String tblName, String filter, short maxParts) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            this.startTableFunction("get_partitions_by_filter", parsedDbName[0], parsedDbName[1], tblName);
            this.fireReadTablePreEvent(parsedDbName[0], parsedDbName[1], tblName);
            List<Partition> ret = null;
            Exception ex = null;

            try {
                this.checkLimitNumberOfPartitionsByFilter(parsedDbName[0], parsedDbName[1], tblName, filter, maxParts);
                ret = this.getMS().getPartitionsByFilter(parsedDbName[0], parsedDbName[1], tblName, filter, maxParts);
            } catch (Exception var12) {
                ex = var12;
                this.rethrowException(var12);
            } finally {
                this.endFunction("get_partitions_by_filter", ret != null, ex, tblName);
            }

            return ret;
        }

        public List<PartitionSpec> get_part_specs_by_filter(String dbName, String tblName, String filter, int maxParts) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            this.startTableFunction("get_partitions_by_filter_pspec", parsedDbName[0], parsedDbName[1], tblName);
            List partitionSpecs = null;

            List var13;
            try {
                Table table = this.get_table_core(parsedDbName[0], parsedDbName[1], tblName);
                List<Partition> partitions = this.get_partitions_by_filter(dbName, tblName, filter, (short)maxParts);
                if (is_partition_spec_grouping_enabled(table)) {
                    partitionSpecs = this.get_partitionspecs_grouped_by_storage_descriptor(table, partitions);
                } else {
                    PartitionSpec pSpec = new PartitionSpec();
                    pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
                    pSpec.setRootPath(table.getSd().getLocation());
                    pSpec.setCatName(parsedDbName[0]);
                    pSpec.setDbName(parsedDbName[1]);
                    pSpec.setTableName(tblName);
                    partitionSpecs = Arrays.asList(pSpec);
                }

                var13 = partitionSpecs;
            } finally {
                this.endFunction("get_partitions_by_filter_pspec", partitionSpecs != null && !partitionSpecs.isEmpty(), (Exception)null, tblName);
            }

            return var13;
        }

        public PartitionsByExprResult get_partitions_by_expr(PartitionsByExprRequest req) throws TException {
            String dbName = req.getDbName();
            String tblName = req.getTblName();
            String catName = req.isSetCatName() ? req.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            this.startTableFunction("get_partitions_by_expr", catName, dbName, tblName);
            this.fireReadTablePreEvent(catName, dbName, tblName);
            PartitionsByExprResult ret = null;
            Exception ex = null;

            try {
                this.checkLimitNumberOfPartitionsByExpr(catName, dbName, tblName, req.getExpr(), -1);
                List<Partition> partitions = new LinkedList();
                boolean hasUnknownPartitions = this.getMS().getPartitionsByExpr(catName, dbName, tblName, req.getExpr(), req.getDefaultPartitionName(), req.getMaxParts(), partitions);
                ret = new PartitionsByExprResult(partitions, hasUnknownPartitions);
            } catch (Exception var12) {
                ex = var12;
                this.rethrowException(var12);
            } finally {
                this.endFunction("get_partitions_by_expr", ret != null, ex, tblName);
            }

            return ret;
        }

        private void rethrowException(Exception e) throws TException {
            if (e instanceof MetaException) {
                throw (MetaException)e;
            } else if (e instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e;
            } else if (e instanceof TException) {
                throw (TException)e;
            } else {
                throw newMetaException(e);
            }
        }

        public int get_num_partitions_by_filter(String dbName, String tblName, String filter) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);
            this.startTableFunction("get_num_partitions_by_filter", parsedDbName[0], parsedDbName[1], tblName);
            int ret = -1;
            Exception ex = null;

            try {
                ret = this.getMS().getNumPartitionsByFilter(parsedDbName[0], parsedDbName[1], tblName, filter);
            } catch (Exception var11) {
                ex = var11;
                this.rethrowException(var11);
            } finally {
                this.endFunction("get_num_partitions_by_filter", ret != -1, ex, tblName);
            }

            return ret;
        }

        private int get_num_partitions_by_expr(String catName, String dbName, String tblName, byte[] expr) throws TException {
            int ret = -1;
            Exception ex = null;

            try {
                ret = this.getMS().getNumPartitionsByExpr(catName, dbName, tblName, expr);
            } catch (Exception var11) {
                ex = var11;
                this.rethrowException(var11);
            } finally {
                this.endFunction("get_num_partitions_by_expr", ret != -1, ex, tblName);
            }

            return ret;
        }

        public List<Partition> get_partitions_by_names(String dbName, String tblName, List<String> partNames) throws TException {
            return this.get_partitions_by_names(dbName, tblName, partNames, false);
        }

        public GetPartitionsByNamesResult get_partitions_by_names_req(GetPartitionsByNamesRequest gpbnr) throws TException {
            List<Partition> partitions = this.get_partitions_by_names(gpbnr.getDb_name(), gpbnr.getTbl_name(), gpbnr.getNames(), gpbnr.isSetGet_col_stats() && gpbnr.isGet_col_stats());
            return new GetPartitionsByNamesResult(partitions);
        }

        public List<Partition> get_partitions_by_names(String dbName, String tblName, List<String> partNames, boolean getColStats) throws TException {
            String[] dbNameParts = MetaStoreUtils.parseDbName(dbName, this.conf);
            String parsedCatName = dbNameParts[0];
            String parsedDbName = dbNameParts[1];
            this.startTableFunction("get_partitions_by_names", parsedCatName, parsedDbName, tblName);
            this.fireReadTablePreEvent(parsedCatName, parsedDbName, tblName);
            List<Partition> ret = null;
            Exception ex = null;
            boolean success = false;

            try {
                this.getMS().openTransaction();
                ret = this.getMS().getPartitionsByNames(parsedCatName, parsedDbName, tblName, partNames);
                if (getColStats) {
                    Table table = this.getTable(parsedCatName, parsedDbName, tblName);
                    Iterator var12 = ret.iterator();

                    while(var12.hasNext()) {
                        Partition part = (Partition)var12.next();
                        String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
                        List<ColumnStatistics> partColStatsList = this.getMS().getPartitionColumnStatistics(parsedCatName, parsedDbName, tblName, Collections.singletonList(partName), StatsSetupConst.getColumnsHavingStats(part.getParameters()));
                        if (partColStatsList != null && !partColStatsList.isEmpty()) {
                            ColumnStatistics partColStats = (ColumnStatistics)partColStatsList.get(0);
                            if (partColStats != null) {
                                part.setColStats(partColStats);
                            }
                        }
                    }
                }

                success = this.getMS().commitTransaction();
            } catch (Exception var20) {
                ex = var20;
                this.rethrowException(var20);
            } finally {
                if (!success) {
                    this.getMS().rollbackTransaction();
                }

                this.endFunction("get_partitions_by_names", ret != null, ex, tblName);
            }

            return ret;
        }

        public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String userName, List<String> groupNames) throws TException {
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String partName;
            if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
                partName = this.getPartName(hiveObject);
                return this.get_column_privilege_set(catName, hiveObject.getDbName(), hiveObject.getObjectName(), partName, hiveObject.getColumnName(), userName, groupNames);
            } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
                partName = this.getPartName(hiveObject);
                return this.get_partition_privilege_set(catName, hiveObject.getDbName(), hiveObject.getObjectName(), partName, userName, groupNames);
            } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
                return this.get_db_privilege_set(catName, hiveObject.getDbName(), userName, groupNames);
            } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
                return this.get_table_privilege_set(catName, hiveObject.getDbName(), hiveObject.getObjectName(), userName, groupNames);
            } else {
                return hiveObject.getObjectType() == HiveObjectType.GLOBAL ? this.get_user_privilege_set(userName, groupNames) : null;
            }
        }

        private String getPartName(HiveObjectRef hiveObject) throws MetaException {
            String partName = null;
            List<String> partValue = hiveObject.getPartValues();
            if (partValue != null && partValue.size() > 0) {
                try {
                    String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
                    Table table = this.get_table_core(catName, hiveObject.getDbName(), hiveObject.getObjectName());
                    partName = Warehouse.makePartName(table.getPartitionKeys(), partValue);
                } catch (NoSuchObjectException var6) {
                    throw new MetaException(var6.getMessage());
                }
            }

            return partName;
        }

        private PrincipalPrivilegeSet get_column_privilege_set(String catName, String dbName, String tableName, String partName, String columnName, String userName, List<String> groupNames) throws TException {
            this.incrementCounter("get_column_privilege_set");

            try {
                PrincipalPrivilegeSet ret = this.getMS().getColumnPrivilegeSet(catName, dbName, tableName, partName, columnName, userName, groupNames);
                return ret;
            } catch (MetaException var10) {
                throw var10;
            } catch (Exception var11) {
                throw new RuntimeException(var11);
            }
        }

        private PrincipalPrivilegeSet get_db_privilege_set(String catName, String dbName, String userName, List<String> groupNames) throws TException {
            this.incrementCounter("get_db_privilege_set");

            try {
                PrincipalPrivilegeSet ret = this.getMS().getDBPrivilegeSet(catName, dbName, userName, groupNames);
                return ret;
            } catch (MetaException var7) {
                throw var7;
            } catch (Exception var8) {
                throw new RuntimeException(var8);
            }
        }

        private PrincipalPrivilegeSet get_partition_privilege_set(String catName, String dbName, String tableName, String partName, String userName, List<String> groupNames) throws TException {
            this.incrementCounter("get_partition_privilege_set");

            try {
                PrincipalPrivilegeSet ret = this.getMS().getPartitionPrivilegeSet(catName, dbName, tableName, partName, userName, groupNames);
                return ret;
            } catch (MetaException var9) {
                throw var9;
            } catch (Exception var10) {
                throw new RuntimeException(var10);
            }
        }

        private PrincipalPrivilegeSet get_table_privilege_set(String catName, String dbName, String tableName, String userName, List<String> groupNames) throws TException {
            this.incrementCounter("get_table_privilege_set");

            try {
                PrincipalPrivilegeSet ret = this.getMS().getTablePrivilegeSet(catName, dbName, tableName, userName, groupNames);
                return ret;
            } catch (MetaException var8) {
                throw var8;
            } catch (Exception var9) {
                throw new RuntimeException(var9);
            }
        }

        public boolean grant_role(String roleName, String principalName, PrincipalType principalType, String grantor, PrincipalType grantorType, boolean grantOption) throws TException {
            this.incrementCounter("add_role_member");
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            if ("public".equals(roleName)) {
                throw new MetaException("No user can be added to public. Since all users implicitly belong to public role.");
            } else {
                Boolean ret;
                try {
                    RawStore ms = this.getMS();
                    Role role = ms.getRole(roleName);
                    if (principalType == PrincipalType.ROLE && this.isNewRoleAParent(principalName, roleName)) {
                        throw new MetaException("Cannot grant role " + principalName + " to " + roleName + " as " + roleName + " already belongs to the role " + principalName + ". (no cycles allowed)");
                    }

                    ret = ms.grantRole(role, principalName, principalType, grantor, grantorType, grantOption);
                } catch (MetaException var10) {
                    throw var10;
                } catch (NoSuchObjectException | InvalidObjectException var11) {
                    ret = false;
                    MetaStoreUtils.logAndThrowMetaException(var11);
                } catch (Exception var12) {
                    throw new TException(var12);
                }

                return ret;
            }
        }

        private boolean isNewRoleAParent(String newRole, String curRole) throws MetaException {
            if (newRole.equals(curRole)) {
                return true;
            } else {
                List<Role> parentRoleMaps = this.getMS().listRoles(curRole, PrincipalType.ROLE);
                Iterator var4 = parentRoleMaps.iterator();

                Role parentRole;
                do {
                    if (!var4.hasNext()) {
                        return false;
                    }

                    parentRole = (Role)var4.next();
                } while(!this.isNewRoleAParent(newRole, parentRole.getRoleName()));

                return true;
            }
        }

        public List<Role> list_roles(String principalName, PrincipalType principalType) throws TException {
            this.incrementCounter("list_roles");
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            return this.getMS().listRoles(principalName, principalType);
        }

        public boolean create_role(Role role) throws TException {
            this.incrementCounter("create_role");
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            if ("public".equals(role.getRoleName())) {
                throw new MetaException("public role implicitly exists. It can't be created.");
            } else {
                Boolean ret;
                try {
                    ret = this.getMS().addRole(role.getRoleName(), role.getOwnerName());
                } catch (MetaException var4) {
                    throw var4;
                } catch (NoSuchObjectException | InvalidObjectException var5) {
                    ret = false;
                    MetaStoreUtils.logAndThrowMetaException(var5);
                } catch (Exception var6) {
                    throw new TException(var6);
                }

                return ret;
            }
        }

        public boolean drop_role(String roleName) throws TException {
            this.incrementCounter("drop_role");
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            if (!"admin".equals(roleName) && !"public".equals(roleName)) {
                Boolean ret;
                try {
                    ret = this.getMS().removeRole(roleName);
                } catch (MetaException var4) {
                    throw var4;
                } catch (NoSuchObjectException var5) {
                    ret = false;
                    MetaStoreUtils.logAndThrowMetaException(var5);
                } catch (Exception var6) {
                    throw new TException(var6);
                }

                return ret;
            } else {
                throw new MetaException("public,admin roles can't be dropped.");
            }
        }

        public List<String> get_role_names() throws TException {
            this.incrementCounter("get_role_names");
            this.firePreEvent(new PreAuthorizationCallEvent(this));

            try {
                List<String> ret = this.getMS().listRoleNames();
                return ret;
            } catch (MetaException var3) {
                throw var3;
            } catch (Exception var4) {
                throw new RuntimeException(var4);
            }
        }

        public boolean grant_privileges(PrivilegeBag privileges) throws TException {
            this.incrementCounter("grant_privileges");
            this.firePreEvent(new PreAuthorizationCallEvent(this));

            Boolean ret;
            try {
                ret = this.getMS().grantPrivileges(privileges);
            } catch (MetaException var4) {
                throw var4;
            } catch (NoSuchObjectException | InvalidObjectException var5) {
                ret = false;
                MetaStoreUtils.logAndThrowMetaException(var5);
            } catch (Exception var6) {
                throw new TException(var6);
            }

            return ret;
        }

        public boolean revoke_role(String roleName, String userName, PrincipalType principalType) throws TException {
            return this.revoke_role(roleName, userName, principalType, false);
        }

        private boolean revoke_role(String roleName, String userName, PrincipalType principalType, boolean grantOption) throws TException {
            this.incrementCounter("remove_role_member");
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            if ("public".equals(roleName)) {
                throw new MetaException("public role can't be revoked.");
            } else {
                Boolean ret;
                try {
                    RawStore ms = this.getMS();
                    Role mRole = ms.getRole(roleName);
                    ret = ms.revokeRole(mRole, userName, principalType, grantOption);
                } catch (MetaException var8) {
                    throw var8;
                } catch (NoSuchObjectException var9) {
                    ret = false;
                    MetaStoreUtils.logAndThrowMetaException(var9);
                } catch (Exception var10) {
                    throw new TException(var10);
                }

                return ret;
            }
        }

        public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request) throws TException {
            GrantRevokeRoleResponse response = new GrantRevokeRoleResponse();
            boolean grantOption = false;
            if (request.isSetGrantOption()) {
                grantOption = request.isGrantOption();
            }

            boolean result;
            switch(request.getRequestType()) {
                case GRANT:
                    result = this.grant_role(request.getRoleName(), request.getPrincipalName(), request.getPrincipalType(), request.getGrantor(), request.getGrantorType(), grantOption);
                    response.setSuccess(result);
                    break;
                case REVOKE:
                    result = this.revoke_role(request.getRoleName(), request.getPrincipalName(), request.getPrincipalType(), grantOption);
                    response.setSuccess(result);
                    break;
                default:
                    throw new MetaException("Unknown request type " + request.getRequestType());
            }

            return response;
        }

        public GrantRevokePrivilegeResponse grant_revoke_privileges(GrantRevokePrivilegeRequest request) throws TException {
            GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
            boolean revokeGrantOption;
            switch(request.getRequestType()) {
                case GRANT:
                    revokeGrantOption = this.grant_privileges(request.getPrivileges());
                    response.setSuccess(revokeGrantOption);
                    break;
                case REVOKE:
                    revokeGrantOption = false;
                    if (request.isSetRevokeGrantOption()) {
                        revokeGrantOption = request.isRevokeGrantOption();
                    }

                    boolean result = this.revoke_privileges(request.getPrivileges(), revokeGrantOption);
                    response.setSuccess(result);
                    break;
                default:
                    throw new MetaException("Unknown request type " + request.getRequestType());
            }

            return response;
        }

        public GrantRevokePrivilegeResponse refresh_privileges(HiveObjectRef objToRefresh, String authorizer, GrantRevokePrivilegeRequest grantRequest) throws TException {
            this.incrementCounter("refresh_privileges");
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();

            try {
                boolean result = this.getMS().refreshPrivileges(objToRefresh, authorizer, grantRequest.getPrivileges());
                response.setSuccess(result);
                return response;
            } catch (MetaException var6) {
                throw var6;
            } catch (Exception var7) {
                throw new RuntimeException(var7);
            }
        }

        public boolean revoke_privileges(PrivilegeBag privileges) throws TException {
            return this.revoke_privileges(privileges, false);
        }

        public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws TException {
            this.incrementCounter("revoke_privileges");
            this.firePreEvent(new PreAuthorizationCallEvent(this));

            Boolean ret;
            try {
                ret = this.getMS().revokePrivileges(privileges, grantOption);
            } catch (MetaException var5) {
                throw var5;
            } catch (NoSuchObjectException | InvalidObjectException var6) {
                ret = false;
                MetaStoreUtils.logAndThrowMetaException(var6);
            } catch (Exception var7) {
                throw new TException(var7);
            }

            return ret;
        }

        private PrincipalPrivilegeSet get_user_privilege_set(String userName, List<String> groupNames) throws TException {
            this.incrementCounter("get_user_privilege_set");

            try {
                PrincipalPrivilegeSet ret = this.getMS().getUserPrivilegeSet(userName, groupNames);
                return ret;
            } catch (MetaException var5) {
                throw var5;
            } catch (Exception var6) {
                throw new RuntimeException(var6);
            }
        }

        public List<HiveObjectPrivilege> list_privileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObject) throws TException {
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            if (hiveObject.getObjectType() == null) {
                return this.getAllPrivileges(principalName, principalType, catName);
            } else if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
                return this.list_global_privileges(principalName, principalType);
            } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
                return this.list_db_privileges(principalName, principalType, catName, hiveObject.getDbName());
            } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
                return this.list_table_privileges(principalName, principalType, catName, hiveObject.getDbName(), hiveObject.getObjectName());
            } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
                return this.list_partition_privileges(principalName, principalType, catName, hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject.getPartValues());
            } else if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
                return hiveObject.getPartValues() != null && !hiveObject.getPartValues().isEmpty() ? this.list_partition_column_privileges(principalName, principalType, catName, hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject.getPartValues(), hiveObject.getColumnName()) : this.list_table_column_privileges(principalName, principalType, catName, hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject.getColumnName());
            } else {
                return null;
            }
        }

        private List<HiveObjectPrivilege> getAllPrivileges(String principalName, PrincipalType principalType, String catName) throws TException {
            List<HiveObjectPrivilege> privs = new ArrayList();
            privs.addAll(this.list_global_privileges(principalName, principalType));
            privs.addAll(this.list_db_privileges(principalName, principalType, catName, (String)null));
            privs.addAll(this.list_table_privileges(principalName, principalType, catName, (String)null, (String)null));
            privs.addAll(this.list_partition_privileges(principalName, principalType, catName, (String)null, (String)null, (List)null));
            privs.addAll(this.list_table_column_privileges(principalName, principalType, catName, (String)null, (String)null, (String)null));
            privs.addAll(this.list_partition_column_privileges(principalName, principalType, catName, (String)null, (String)null, (List)null, (String)null));
            return privs;
        }

        private List<HiveObjectPrivilege> list_table_column_privileges(String principalName, PrincipalType principalType, String catName, String dbName, String tableName, String columnName) throws TException {
            this.incrementCounter("list_table_column_privileges");

            try {
                if (dbName == null) {
                    return this.getMS().listPrincipalTableColumnGrantsAll(principalName, principalType);
                } else {
                    return principalName == null ? this.getMS().listTableColumnGrantsAll(catName, dbName, tableName, columnName) : this.getMS().listPrincipalTableColumnGrants(principalName, principalType, catName, dbName, tableName, columnName);
                }
            } catch (MetaException var8) {
                throw var8;
            } catch (Exception var9) {
                throw new RuntimeException(var9);
            }
        }

        private List<HiveObjectPrivilege> list_partition_column_privileges(String principalName, PrincipalType principalType, String catName, String dbName, String tableName, List<String> partValues, String columnName) throws TException {
            this.incrementCounter("list_partition_column_privileges");

            try {
                if (dbName == null) {
                    return this.getMS().listPrincipalPartitionColumnGrantsAll(principalName, principalType);
                } else {
                    Table tbl = this.get_table_core(catName, dbName, tableName);
                    String partName = Warehouse.makePartName(tbl.getPartitionKeys(), partValues);
                    return principalName == null ? this.getMS().listPartitionColumnGrantsAll(catName, dbName, tableName, partName, columnName) : this.getMS().listPrincipalPartitionColumnGrants(principalName, principalType, catName, dbName, tableName, partValues, partName, columnName);
                }
            } catch (MetaException var10) {
                throw var10;
            } catch (Exception var11) {
                throw new RuntimeException(var11);
            }
        }

        private List<HiveObjectPrivilege> list_db_privileges(String principalName, PrincipalType principalType, String catName, String dbName) throws TException {
            this.incrementCounter("list_security_db_grant");

            try {
                if (dbName == null) {
                    return this.getMS().listPrincipalDBGrantsAll(principalName, principalType);
                } else {
                    return principalName == null ? this.getMS().listDBGrantsAll(catName, dbName) : this.getMS().listPrincipalDBGrants(principalName, principalType, catName, dbName);
                }
            } catch (MetaException var6) {
                throw var6;
            } catch (Exception var7) {
                throw new RuntimeException(var7);
            }
        }

        private List<HiveObjectPrivilege> list_partition_privileges(String principalName, PrincipalType principalType, String catName, String dbName, String tableName, List<String> partValues) throws TException {
            this.incrementCounter("list_security_partition_grant");

            try {
                if (dbName == null) {
                    return this.getMS().listPrincipalPartitionGrantsAll(principalName, principalType);
                } else {
                    Table tbl = this.get_table_core(catName, dbName, tableName);
                    String partName = Warehouse.makePartName(tbl.getPartitionKeys(), partValues);
                    return principalName == null ? this.getMS().listPartitionGrantsAll(catName, dbName, tableName, partName) : this.getMS().listPrincipalPartitionGrants(principalName, principalType, catName, dbName, tableName, partValues, partName);
                }
            } catch (MetaException var9) {
                throw var9;
            } catch (Exception var10) {
                throw new RuntimeException(var10);
            }
        }

        private List<HiveObjectPrivilege> list_table_privileges(String principalName, PrincipalType principalType, String catName, String dbName, String tableName) throws TException {
            this.incrementCounter("list_security_table_grant");

            try {
                if (dbName == null) {
                    return this.getMS().listPrincipalTableGrantsAll(principalName, principalType);
                } else {
                    return principalName == null ? this.getMS().listTableGrantsAll(catName, dbName, tableName) : this.getMS().listAllTableGrants(principalName, principalType, catName, dbName, tableName);
                }
            } catch (MetaException var7) {
                throw var7;
            } catch (Exception var8) {
                throw new RuntimeException(var8);
            }
        }

        private List<HiveObjectPrivilege> list_global_privileges(String principalName, PrincipalType principalType) throws TException {
            this.incrementCounter("list_security_user_grant");

            try {
                return principalName == null ? this.getMS().listGlobalGrantsAll() : this.getMS().listPrincipalGlobalGrants(principalName, principalType);
            } catch (MetaException var4) {
                throw var4;
            } catch (Exception var5) {
                throw new RuntimeException(var5);
            }
        }

        public void cancel_delegation_token(String token_str_form) throws TException {
            this.startFunction("cancel_delegation_token");
            boolean success = false;
            Object ex = null;

            try {
                HiveMetaStore.cancelDelegationToken(token_str_form);
                success = true;
            } catch (IOException var9) {
                ex = var9;
                throw new MetaException(var9.getMessage());
            } catch (Exception var10) {
                ex = var10;
                throw newMetaException(var10);
            } finally {
                this.endFunction("cancel_delegation_token", success, (Exception)ex);
            }

        }

        public long renew_delegation_token(String token_str_form) throws TException {
            this.startFunction("renew_delegation_token");
            Long ret = null;
            Object ex = null;

            try {
                ret = HiveMetaStore.renewDelegationToken(token_str_form);
            } catch (IOException var9) {
                ex = var9;
                throw new MetaException(var9.getMessage());
            } catch (Exception var10) {
                ex = var10;
                throw newMetaException(var10);
            } finally {
                this.endFunction("renew_delegation_token", ret != null, (Exception)ex);
            }

            return ret;
        }

        public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name) throws TException {
            this.startFunction("get_delegation_token");
            String ret = null;
            Object ex = null;

            try {
                ret = HiveMetaStore.getDelegationToken(token_owner, renewer_kerberos_principal_name, getIPAddress());
            } catch (InterruptedException | IOException var10) {
                ex = var10;
                throw new MetaException(var10.getMessage());
            } catch (Exception var11) {
                ex = var11;
                throw newMetaException(var11);
            } finally {
                this.endFunction("get_delegation_token", ret != null, (Exception)ex);
            }

            return ret;
        }

        public boolean add_token(String token_identifier, String delegation_token) throws TException {
            this.startFunction("add_token", ": " + token_identifier);
            boolean ret = false;
            Exception ex = null;

            try {
                ret = this.getMS().addToken(token_identifier, delegation_token);
            } catch (Exception var9) {
                ex = var9;
                if (var9 instanceof MetaException) {
                    throw (MetaException)var9;
                }

                throw newMetaException(var9);
            } finally {
                this.endFunction("add_token", ret, ex);
            }

            return ret;
        }

        public boolean remove_token(String token_identifier) throws TException {
            this.startFunction("remove_token", ": " + token_identifier);
            boolean ret = false;
            Exception ex = null;

            try {
                ret = this.getMS().removeToken(token_identifier);
            } catch (Exception var8) {
                ex = var8;
                if (var8 instanceof MetaException) {
                    throw (MetaException)var8;
                }

                throw newMetaException(var8);
            } finally {
                this.endFunction("remove_token", ret, ex);
            }

            return ret;
        }

        public String get_token(String token_identifier) throws TException {
            this.startFunction("get_token for", ": " + token_identifier);
            String ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().getToken(token_identifier);
            } catch (Exception var8) {
                ex = var8;
                if (var8 instanceof MetaException) {
                    throw (MetaException)var8;
                }

                throw newMetaException(var8);
            } finally {
                this.endFunction("get_token", ret != null, ex);
            }

            return ret == null ? "" : ret;
        }

        public List<String> get_all_token_identifiers() throws TException {
            this.startFunction("get_all_token_identifiers.");
            Exception ex = null;

            List ret;
            try {
                ret = this.getMS().getAllTokenIdentifiers();
            } catch (Exception var7) {
                ex = var7;
                if (var7 instanceof MetaException) {
                    throw (MetaException)var7;
                }

                throw newMetaException(var7);
            } finally {
                this.endFunction("get_all_token_identifiers.", ex == null, ex);
            }

            return ret;
        }

        public int add_master_key(String key) throws TException {
            this.startFunction("add_master_key.");
            Exception ex = null;

            int ret;
            try {
                ret = this.getMS().addMasterKey(key);
            } catch (Exception var8) {
                ex = var8;
                if (var8 instanceof MetaException) {
                    throw (MetaException)var8;
                }

                throw newMetaException(var8);
            } finally {
                this.endFunction("add_master_key.", ex == null, ex);
            }

            return ret;
        }

        public void update_master_key(int seq_number, String key) throws TException {
            this.startFunction("update_master_key.");
            Exception ex = null;

            try {
                this.getMS().updateMasterKey(seq_number, key);
            } catch (Exception var8) {
                ex = var8;
                if (var8 instanceof MetaException) {
                    throw (MetaException)var8;
                }

                throw newMetaException(var8);
            } finally {
                this.endFunction("update_master_key.", ex == null, ex);
            }

        }

        public boolean remove_master_key(int key_seq) throws TException {
            this.startFunction("remove_master_key.");
            Exception ex = null;

            boolean ret;
            try {
                ret = this.getMS().removeMasterKey(key_seq);
            } catch (Exception var8) {
                ex = var8;
                if (var8 instanceof MetaException) {
                    throw (MetaException)var8;
                }

                throw newMetaException(var8);
            } finally {
                this.endFunction("remove_master_key.", ex == null, ex);
            }

            return ret;
        }

        public List<String> get_master_keys() throws TException {
            this.startFunction("get_master_keys.");
            Exception ex = null;
            String[] ret = null;

            try {
                ret = this.getMS().getMasterKeys();
            } catch (Exception var7) {
                ex = var7;
                if (var7 instanceof MetaException) {
                    throw (MetaException)var7;
                }

                throw newMetaException(var7);
            } finally {
                this.endFunction("get_master_keys.", ret != null, ex);
            }

            return Arrays.asList(ret);
        }

        public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partName, PartitionEventType evtType) throws TException {
            Table tbl = null;
            Exception ex = null;
            RawStore ms = this.getMS();
            boolean success = false;

            try {
                String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
                ms.openTransaction();
                this.startPartitionFunction("markPartitionForEvent", parsedDbName[0], parsedDbName[1], tbl_name, partName);
                this.firePreEvent(new PreLoadPartitionDoneEvent(parsedDbName[0], parsedDbName[1], tbl_name, partName, this));
                tbl = ms.markPartitionForEvent(parsedDbName[0], parsedDbName[1], tbl_name, partName, evtType);
                if (null == tbl) {
                    throw new UnknownTableException("Table: " + tbl_name + " not found.");
                }

                if (this.transactionalListeners.size() > 0) {
                    LoadPartitionDoneEvent lpde = new LoadPartitionDoneEvent(true, tbl, partName, this);
                    Iterator var11 = this.transactionalListeners.iterator();

                    while(var11.hasNext()) {
                        MetaStoreEventListener transactionalListener = (MetaStoreEventListener)var11.next();
                        transactionalListener.onLoadPartitionDone(lpde);
                    }
                }

                success = ms.commitTransaction();
                Iterator var18 = this.listeners.iterator();

                while(var18.hasNext()) {
                    MetaStoreEventListener listener = (MetaStoreEventListener)var18.next();
                    listener.onLoadPartitionDone(new LoadPartitionDoneEvent(true, tbl, partName, this));
                }
            } catch (Exception var16) {
                ex = var16;
                LOG.error("Exception caught in mark partition event ", var16);
                if (var16 instanceof UnknownTableException) {
                    throw (UnknownTableException)var16;
                }

                if (var16 instanceof UnknownPartitionException) {
                    throw (UnknownPartitionException)var16;
                }

                if (var16 instanceof InvalidPartitionException) {
                    throw (InvalidPartitionException)var16;
                }

                if (var16 instanceof MetaException) {
                    throw (MetaException)var16;
                }

                throw newMetaException(var16);
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                }

                this.endFunction("markPartitionForEvent", tbl != null, ex, tbl_name);
            }

        }

        public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partName, PartitionEventType evtType) throws TException {
            String[] parsedDbName = MetaStoreUtils.parseDbName(db_name, this.conf);
            this.startPartitionFunction("isPartitionMarkedForEvent", parsedDbName[0], parsedDbName[1], tbl_name, partName);
            Boolean ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().isPartitionMarkedForEvent(parsedDbName[0], parsedDbName[1], tbl_name, partName, evtType);
            } catch (Exception var12) {
                LOG.error("Exception caught for isPartitionMarkedForEvent ", var12);
                ex = var12;
                if (var12 instanceof UnknownTableException) {
                    throw (UnknownTableException)var12;
                }

                if (var12 instanceof UnknownPartitionException) {
                    throw (UnknownPartitionException)var12;
                }

                if (var12 instanceof InvalidPartitionException) {
                    throw (InvalidPartitionException)var12;
                }

                if (var12 instanceof MetaException) {
                    throw (MetaException)var12;
                }

                throw newMetaException(var12);
            } finally {
                this.endFunction("isPartitionMarkedForEvent", ret != null, ex, tbl_name);
            }

            return ret;
        }

        public List<String> set_ugi(String username, List<String> groupNames) throws TException {
            Collections.addAll(groupNames, new String[]{username});
            return groupNames;
        }

        public boolean partition_name_has_valid_characters(List<String> part_vals, boolean throw_exception) throws TException {
            this.startFunction("partition_name_has_valid_characters");
            Object ex = null;

            boolean ret;
            try {
                if (throw_exception) {
                    MetaStoreUtils.validatePartitionNameCharacters(part_vals, this.partitionValidationPattern);
                    ret = true;
                } else {
                    ret = MetaStoreUtils.partitionNameHasValidCharacters(part_vals, this.partitionValidationPattern);
                }
            } catch (Exception var6) {
                if (var6 instanceof MetaException) {
                    throw (MetaException)var6;
                }

                throw newMetaException(var6);
            }

            this.endFunction("partition_name_has_valid_characters", true, (Exception)ex);
            return ret;
        }

        private static MetaException newMetaException(Exception e) {
            if (e instanceof MetaException) {
                return (MetaException)e;
            } else {
                MetaException me = new MetaException(e.toString());
                me.initCause(e);
                return me;
            }
        }

        private void validateFunctionInfo(org.apache.hadoop.hive.metastore.api.Function func) throws InvalidObjectException, MetaException {
            if (!MetaStoreUtils.validateName(func.getFunctionName(), (Configuration)null)) {
                throw new InvalidObjectException(func.getFunctionName() + " is not a valid object name");
            } else {
                String className = func.getClassName();
                if (className == null) {
                    throw new InvalidObjectException("Function class name cannot be null");
                }
            }
        }

        public void create_function(org.apache.hadoop.hive.metastore.api.Function func) throws TException {
            this.validateFunctionInfo(func);
            boolean success = false;
            RawStore ms = this.getMS();
            Map transactionalListenerResponses = Collections.emptyMap();

            try {
                String catName = func.isSetCatName() ? func.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
                ms.openTransaction();
                Database db = ms.getDatabase(catName, func.getDbName());
                if (db == null) {
                    throw new NoSuchObjectException("The database " + func.getDbName() + " does not exist");
                }

                org.apache.hadoop.hive.metastore.api.Function existingFunc = ms.getFunction(catName, func.getDbName(), func.getFunctionName());
                if (existingFunc != null) {
                    throw new AlreadyExistsException("Function " + func.getFunctionName() + " already exists");
                }

                long time = System.currentTimeMillis() / 1000L;
                func.setCreateTime((int)time);
                ms.createFunction(func);
                if (!this.transactionalListeners.isEmpty()) {
                    transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.CREATE_FUNCTION, new CreateFunctionEvent(func, true, this));
                }

                success = ms.commitTransaction();
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                }

                if (!this.listeners.isEmpty()) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.CREATE_FUNCTION, new CreateFunctionEvent(func, success, this), (EnvironmentContext)null, transactionalListenerResponses, ms);
                }

            }

        }

        public void drop_function(String dbName, String funcName) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
            boolean success = false;
            org.apache.hadoop.hive.metastore.api.Function func = null;
            RawStore ms = this.getMS();
            Map<String, String> transactionalListenerResponses = Collections.emptyMap();
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);

            try {
                ms.openTransaction();
                func = ms.getFunction(parsedDbName[0], parsedDbName[1], funcName);
                if (func == null) {
                    throw new NoSuchObjectException("Function " + funcName + " does not exist");
                }

                Boolean isSourceOfReplication = ReplChangeManager.isSourceOfReplication(this.get_database_core(parsedDbName[0], parsedDbName[1]));
                if (func.getResourceUris() != null && !func.getResourceUris().isEmpty()) {
                    Iterator var9 = func.getResourceUris().iterator();

                    while(var9.hasNext()) {
                        ResourceUri uri = (ResourceUri)var9.next();
                        if (uri.getUri().toLowerCase().startsWith("hdfs:") && isSourceOfReplication) {
                            this.wh.addToChangeManagement(new Path(uri.getUri()));
                        }
                    }
                }

                ms.dropFunction(parsedDbName[0], parsedDbName[1], funcName);
                if (this.transactionalListeners.size() > 0) {
                    transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.DROP_FUNCTION, new DropFunctionEvent(func, true, this));
                }

                success = ms.commitTransaction();
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                }

                if (this.listeners.size() > 0) {
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.DROP_FUNCTION, new DropFunctionEvent(func, success, this), (EnvironmentContext)null, transactionalListenerResponses, ms);
                }

            }

        }

        public void alter_function(String dbName, String funcName, org.apache.hadoop.hive.metastore.api.Function newFunc) throws TException {
            this.validateFunctionInfo(newFunc);
            boolean success = false;
            RawStore ms = this.getMS();
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);

            try {
                ms.openTransaction();
                ms.alterFunction(parsedDbName[0], parsedDbName[1], funcName, newFunc);
                success = ms.commitTransaction();
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                }

            }

        }

        public List<String> get_functions(String dbName, String pattern) throws MetaException {
            this.startFunction("get_functions", ": db=" + dbName + " pat=" + pattern);
            RawStore ms = this.getMS();
            Exception ex = null;
            List<String> funcNames = null;
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);

            try {
                funcNames = ms.getFunctions(parsedDbName[0], parsedDbName[1], pattern);
            } catch (Exception var11) {
                ex = var11;
                throw newMetaException(var11);
            } finally {
                this.endFunction("get_functions", funcNames != null, ex);
            }

            return funcNames;
        }

        public GetAllFunctionsResponse get_all_functions() throws MetaException {
            GetAllFunctionsResponse response = new GetAllFunctionsResponse();
            this.startFunction("get_all_functions");
            RawStore ms = this.getMS();
            List<org.apache.hadoop.hive.metastore.api.Function> allFunctions = null;
            Exception ex = null;

            try {
                allFunctions = ms.getAllFunctions("hive");
            } catch (Exception var9) {
                ex = var9;
                throw newMetaException(var9);
            } finally {
                this.endFunction("get_all_functions", allFunctions != null, ex);
            }

            response.setFunctions(allFunctions);
            return response;
        }

        public org.apache.hadoop.hive.metastore.api.Function get_function(String dbName, String funcName) throws TException {
            this.startFunction("get_function", ": " + dbName + "." + funcName);
            RawStore ms = this.getMS();
            org.apache.hadoop.hive.metastore.api.Function func = null;
            Exception ex = null;
            String[] parsedDbName = MetaStoreUtils.parseDbName(dbName, this.conf);

            try {
                func = ms.getFunction(parsedDbName[0], parsedDbName[1], funcName);
                if (func == null) {
                    throw new NoSuchObjectException("Function " + dbName + "." + funcName + " does not exist");
                }
            } catch (NoSuchObjectException var12) {
                ex = var12;
                this.rethrowException(var12);
            } catch (Exception var13) {
                ex = var13;
                throw newMetaException(var13);
            } finally {
                this.endFunction("get_function", func != null, (Exception)ex);
            }

            return func;
        }

        public GetOpenTxnsResponse get_open_txns() throws TException {
            return this.getTxnHandler().getOpenTxns();
        }

        public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
            return this.getTxnHandler().getOpenTxnsInfo();
        }

        public OpenTxnsResponse open_txns(OpenTxnRequest rqst) throws TException {
            OpenTxnsResponse response = this.getTxnHandler().openTxns(rqst);
            List<Long> txnIds = response.getTxn_ids();
            if (txnIds != null && this.listeners != null && !this.listeners.isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.OPEN_TXN, new OpenTxnEvent(txnIds, this));
            }

            return response;
        }

        public void abort_txn(AbortTxnRequest rqst) throws TException {
            this.getTxnHandler().abortTxn(rqst);
            if (this.listeners != null && !this.listeners.isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ABORT_TXN, new AbortTxnEvent(rqst.getTxnid(), this));
            }

        }

        public void abort_txns(AbortTxnsRequest rqst) throws TException {
            this.getTxnHandler().abortTxns(rqst);
            if (this.listeners != null && !this.listeners.isEmpty()) {
                Iterator var2 = rqst.getTxn_ids().iterator();

                while(var2.hasNext()) {
                    Long txnId = (Long)var2.next();
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ABORT_TXN, new AbortTxnEvent(txnId, this));
                }
            }

        }

        public void commit_txn(CommitTxnRequest rqst) throws TException {
            if (rqst.isSetWriteEventInfos()) {
                long targetTxnId = this.getTxnHandler().getTargetTxnId(rqst.getReplPolicy(), rqst.getTxnid());
                if (targetTxnId < 0L) {
                    return;
                }

                Partition ptnObj;
                Table tbl;
                WriteNotificationLogRequest wnRqst;
                for(Iterator var4 = rqst.getWriteEventInfos().iterator(); var4.hasNext(); this.addTxnWriteNotificationLog(tbl, ptnObj, wnRqst)) {
                    WriteEventInfo writeEventInfo = (WriteEventInfo)var4.next();
                    String[] filesAdded = ReplChangeManager.getListFromSeparatedString(writeEventInfo.getFiles());
                    List<String> partitionValue = null;
                    ptnObj = null;
                    tbl = this.getTblObject(writeEventInfo.getDatabase(), writeEventInfo.getTable());
                    String root;
                    if (writeEventInfo.getPartition() != null && !writeEventInfo.getPartition().isEmpty()) {
                        partitionValue = Warehouse.getPartValuesFromPartName(writeEventInfo.getPartition());
                        ptnObj = this.getPartitionObj(writeEventInfo.getDatabase(), writeEventInfo.getTable(), partitionValue, tbl);
                        root = ptnObj.getSd().getLocation();
                    } else {
                        root = tbl.getSd().getLocation();
                    }

                    InsertEventRequestData insertData = new InsertEventRequestData();
                    insertData.setReplace(true);
                    String[] var12 = filesAdded;
                    int var13 = filesAdded.length;

                    for(int var14 = 0; var14 < var13; ++var14) {
                        String file = var12[var14];
                        String[] decodedPath = ReplChangeManager.decodeFileUri(file);
                        String name = (new Path(decodedPath[0])).getName();
                        Path newPath = FileUtils.getTransformedPath(name, decodedPath[3], root);
                        insertData.addToFilesAdded(newPath.toUri().toString());
                        insertData.addToSubDirectoryList(decodedPath[3]);

                        try {
                            insertData.addToFilesAddedChecksum(ReplChangeManager.checksumFor(newPath, newPath.getFileSystem(this.conf)));
                        } catch (IOException var20) {
                            LOG.error("failed to get checksum for the file " + newPath + " with error: " + var20.getMessage());
                            throw new TException(var20.getMessage());
                        }
                    }

                    wnRqst = new WriteNotificationLogRequest(targetTxnId, writeEventInfo.getWriteId(), writeEventInfo.getDatabase(), writeEventInfo.getTable(), insertData);
                    if (partitionValue != null) {
                        wnRqst.setPartitionVals(partitionValue);
                    }
                }
            }

            this.getTxnHandler().commitTxn(rqst);
            if (this.listeners != null && !this.listeners.isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.COMMIT_TXN, new CommitTxnEvent(rqst.getTxnid(), this));
            }

        }

        public void repl_tbl_writeid_state(ReplTblWriteIdStateRequest rqst) throws TException {
            this.getTxnHandler().replTableWriteIdState(rqst);
        }

        public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest rqst) throws TException {
            return this.getTxnHandler().getValidWriteIds(rqst);
        }

        public AllocateTableWriteIdsResponse allocate_table_write_ids(AllocateTableWriteIdsRequest rqst) throws TException {
            AllocateTableWriteIdsResponse response = this.getTxnHandler().allocateTableWriteIds(rqst);
            if (this.listeners != null && !this.listeners.isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALLOC_WRITE_ID, new AllocWriteIdEvent(response.getTxnToWriteIds(), rqst.getDbName(), rqst.getTableName(), this));
            }

            return response;
        }

        private void addTxnWriteNotificationLog(Table tableObj, Partition ptnObj, WriteNotificationLogRequest rqst) throws MetaException {
            String partition = "";
            if (ptnObj != null) {
                partition = Warehouse.makePartName(tableObj.getPartitionKeys(), rqst.getPartitionVals());
            }

            AcidWriteEvent event = new AcidWriteEvent(partition, tableObj, ptnObj, rqst);
            this.getTxnHandler().addWriteNotificationLog(event);
            if (this.listeners != null && !this.listeners.isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ACID_WRITE, event);
            }

        }

        private Table getTblObject(String db, String table) throws MetaException, NoSuchObjectException {
            GetTableRequest req = new GetTableRequest(db, table);
            req.setCapabilities(new ClientCapabilities(Lists.newArrayList(new ClientCapability[]{ClientCapability.TEST_CAPABILITY, ClientCapability.INSERT_ONLY_TABLES})));
            return this.get_table_req(req).getTable();
        }

        private Partition getPartitionObj(String db, String table, List<String> partitionVals, Table tableObj) throws MetaException, NoSuchObjectException {
            return tableObj.isSetPartitionKeys() && !tableObj.getPartitionKeys().isEmpty() ? this.get_partition(db, table, partitionVals) : null;
        }

        public WriteNotificationLogResponse add_write_notification_log(WriteNotificationLogRequest rqst) throws TException {
            Table tableObj = this.getTblObject(rqst.getDb(), rqst.getTable());
            Partition ptnObj = this.getPartitionObj(rqst.getDb(), rqst.getTable(), rqst.getPartitionVals(), tableObj);
            this.addTxnWriteNotificationLog(tableObj, ptnObj, rqst);
            return new WriteNotificationLogResponse();
        }

        public LockResponse lock(LockRequest rqst) throws TException {
            return this.getTxnHandler().lock(rqst);
        }

        public LockResponse check_lock(CheckLockRequest rqst) throws TException {
            return this.getTxnHandler().checkLock(rqst);
        }

        public void unlock(UnlockRequest rqst) throws TException {
            this.getTxnHandler().unlock(rqst);
        }

        public ShowLocksResponse show_locks(ShowLocksRequest rqst) throws TException {
            return this.getTxnHandler().showLocks(rqst);
        }

        public void heartbeat(HeartbeatRequest ids) throws TException {
            this.getTxnHandler().heartbeat(ids);
        }

        public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest rqst) throws TException {
            return this.getTxnHandler().heartbeatTxnRange(rqst);
        }

        /** @deprecated */
        @Deprecated
        public void compact(CompactionRequest rqst) throws TException {
            this.compact2(rqst);
        }

        public CompactionResponse compact2(CompactionRequest rqst) throws TException {
            return this.getTxnHandler().compact(rqst);
        }

        public ShowCompactResponse show_compact(ShowCompactRequest rqst) throws TException {
            return this.getTxnHandler().showCompact(rqst);
        }

        public void flushCache() throws TException {
            this.getMS().flushCache();
        }

        public void add_dynamic_partitions(AddDynamicPartitions rqst) throws TException {
            this.getTxnHandler().addDynamicPartitions(rqst);
        }

        public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request) throws TException {
            this.incrementCounter("get_principals_in_role");
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            Exception ex = null;
            GetPrincipalsInRoleResponse response = null;

            try {
                response = new GetPrincipalsInRoleResponse(this.getMS().listRoleMembers(request.getRoleName()));
            } catch (MetaException var9) {
                throw var9;
            } catch (Exception var10) {
                ex = var10;
                this.rethrowException(var10);
            } finally {
                this.endFunction("get_principals_in_role", ex == null, ex);
            }

            return response;
        }

        public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(GetRoleGrantsForPrincipalRequest request) throws TException {
            this.incrementCounter("get_role_grants_for_principal");
            this.firePreEvent(new PreAuthorizationCallEvent(this));
            Exception ex = null;
            List roleMaps = null;

            try {
                roleMaps = this.getMS().listRolesWithGrants(request.getPrincipal_name(), request.getPrincipal_type());
            } catch (MetaException var9) {
                throw var9;
            } catch (Exception var10) {
                ex = var10;
                this.rethrowException(var10);
            } finally {
                this.endFunction("get_role_grants_for_principal", ex == null, ex);
            }

            return new GetRoleGrantsForPrincipalResponse(roleMaps);
        }

        public AggrStats get_aggr_stats_for(PartitionsStatsRequest request) throws TException {
            String catName = request.isSetCatName() ? request.getCatName().toLowerCase() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String dbName = request.getDbName().toLowerCase();
            String tblName = request.getTblName().toLowerCase();
            this.startFunction("get_aggr_stats_for", ": table=" + Warehouse.getCatalogQualifiedTableName(catName, dbName, tblName));
            List<String> lowerCaseColNames = new ArrayList(request.getColNames().size());
            Iterator var6 = request.getColNames().iterator();

            while(var6.hasNext()) {
                String colName = (String)var6.next();
                lowerCaseColNames.add(colName.toLowerCase());
            }

            List<String> lowerCasePartNames = new ArrayList(request.getPartNames().size());
            Iterator var13 = request.getPartNames().iterator();

            while(var13.hasNext()) {
                String partName = (String)var13.next();
                lowerCasePartNames.add(this.lowerCaseConvertPartName(partName));
            }

            AggrStats aggrStats = null;

            AggrStats var15;
            try {
                aggrStats = this.getMS().get_aggr_stats_for(catName, dbName, tblName, lowerCasePartNames, lowerCaseColNames, request.getValidWriteIdList());
                var15 = aggrStats;
            } finally {
                this.endFunction("get_aggr_stats_for", aggrStats == null, (Exception)null, request.getTblName());
            }

            return var15;
        }

        public boolean set_aggr_stats_for(SetPartitionsStatsRequest request) throws TException {
            boolean ret = true;
            List<ColumnStatistics> csNews = request.getColStats();
            if (csNews != null && !csNews.isEmpty()) {
                ColumnStatistics firstColStats = (ColumnStatistics)csNews.get(0);
                ColumnStatisticsDesc statsDesc = firstColStats.getStatsDesc();
                String catName = statsDesc.isSetCatName() ? statsDesc.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
                String dbName = statsDesc.getDbName();
                String tableName = statsDesc.getTableName();
                List<String> colNames = new ArrayList();
                Iterator var10 = firstColStats.getStatsObj().iterator();

                while(var10.hasNext()) {
                    ColumnStatisticsObj obj = (ColumnStatisticsObj)var10.next();
                    colNames.add(obj.getColName());
                }

                if (statsDesc.isIsTblLevel()) {
                    if (request.getColStatsSize() != 1) {
                        throw new MetaException("Expecting only 1 ColumnStatistics for table's column stats, but find " + request.getColStatsSize());
                    } else {
                        return request.isSetNeedMerge() && request.isNeedMerge() ? this.updateTableColumnStatsWithMerge(catName, dbName, tableName, colNames, request) : this.updateTableColumnStatsInternal(firstColStats, request.getValidWriteIdList(), request.getWriteId());
                    }
                } else {
                    Map<String, ColumnStatistics> newStatsMap = new HashMap();

                    ColumnStatistics csNew;
                    String partName;
                    for(Iterator var15 = csNews.iterator(); var15.hasNext(); newStatsMap.put(partName, csNew)) {
                        csNew = (ColumnStatistics)var15.next();
                        partName = csNew.getStatsDesc().getPartName();
                        if (newStatsMap.containsKey(partName)) {
                            MetaStoreUtils.mergeColStats(csNew, (ColumnStatistics)newStatsMap.get(partName));
                        }
                    }

                    if (request.isSetNeedMerge() && request.isNeedMerge()) {
                        ret = this.updatePartColumnStatsWithMerge(catName, dbName, tableName, colNames, newStatsMap, request);
                    } else {
                        Table t = this.getTable(catName, dbName, tableName);

                        Entry entry;
                        for(Iterator var17 = newStatsMap.entrySet().iterator(); var17.hasNext(); ret = this.updatePartitonColStatsInternal(t, (ColumnStatistics)entry.getValue(), request.getValidWriteIdList(), request.getWriteId()) && ret) {
                            entry = (Entry)var17.next();
                        }
                    }

                    return ret;
                }
            } else {
                return ret;
            }
        }

        private boolean updatePartColumnStatsWithMerge(String catName, String dbName, String tableName, List<String> colNames, Map<String, ColumnStatistics> newStatsMap, SetPartitionsStatsRequest request) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
            RawStore ms = this.getMS();
            ms.openTransaction();
            boolean isCommitted = false;
            boolean result = false;

            try {
                List<String> partitionNames = new ArrayList();
                partitionNames.addAll(newStatsMap.keySet());
                List<ColumnStatistics> csOlds = ms.getPartitionColumnStatistics(catName, dbName, tableName, partitionNames, colNames, request.getValidWriteIdList());
                if (newStatsMap.values().size() != csOlds.size()) {
                    LOG.debug("Some of the partitions miss stats.");
                }

                Map<String, ColumnStatistics> oldStatsMap = new HashMap();
                Iterator var13 = csOlds.iterator();

                while(var13.hasNext()) {
                    ColumnStatistics csOld = (ColumnStatistics)var13.next();
                    oldStatsMap.put(csOld.getStatsDesc().getPartName(), csOld);
                }

                List<Partition> partitions = ms.getPartitionsByNames(catName, dbName, tableName, partitionNames);
                Map<String, Partition> mapToPart = new HashMap();

                for(int index = 0; index < partitionNames.size(); ++index) {
                    mapToPart.put(partitionNames.get(index), partitions.get(index));
                }

                Table t = this.getTable(catName, dbName, tableName);
                Iterator var16 = newStatsMap.entrySet().iterator();

                while(true) {
                    while(var16.hasNext()) {
                        Entry<String, ColumnStatistics> entry = (Entry)var16.next();
                        ColumnStatistics csNew = (ColumnStatistics)entry.getValue();
                        ColumnStatistics csOld = (ColumnStatistics)oldStatsMap.get(entry.getKey());
                        boolean isInvalidTxnStats = csOld != null && csOld.isSetIsStatsCompliant() && !csOld.isIsStatsCompliant();
                        Partition part = (Partition)mapToPart.get(entry.getKey());
                        if (isInvalidTxnStats) {
                            csNew.setStatsObj(Lists.newArrayList());
                        } else {
                            MetaStoreUtils.getMergableCols(csNew, part.getParameters());
                            if (csOld != null && csOld.getStatsObjSize() != 0 && !csNew.getStatsObj().isEmpty()) {
                                MetaStoreUtils.mergeColStats(csNew, csOld);
                            }
                        }

                        if (!csNew.getStatsObj().isEmpty()) {
                            result = this.updatePartitonColStatsInternal(t, csNew, request.getValidWriteIdList(), request.getWriteId()) && result;
                        } else if (isInvalidTxnStats) {
                            part.setWriteId(request.getWriteId());
                            StatsSetupConst.clearColumnStatsState(part.getParameters());
                            StatsSetupConst.setBasicStatsState(part.getParameters(), "false");
                            ms.alterPartition(catName, dbName, tableName, part.getValues(), part, request.getValidWriteIdList());
                            result = false;
                        } else {
                            LOG.debug("All the column stats " + csNew.getStatsDesc().getPartName() + " are not accurate to merge.");
                        }
                    }

                    ms.commitTransaction();
                    isCommitted = true;
                    return result;
                }
            } finally {
                if (!isCommitted) {
                    ms.rollbackTransaction();
                }

            }
        }

        private boolean updateTableColumnStatsWithMerge(String catName, String dbName, String tableName, List<String> colNames, SetPartitionsStatsRequest request) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
            ColumnStatistics firstColStats = (ColumnStatistics)request.getColStats().get(0);
            RawStore ms = this.getMS();
            ms.openTransaction();
            boolean isCommitted = false;
            boolean result = false;

            try {
                ColumnStatistics csOld = ms.getTableColumnStatistics(catName, dbName, tableName, colNames, request.getValidWriteIdList());
                boolean isInvalidTxnStats = csOld != null && csOld.isSetIsStatsCompliant() && !csOld.isIsStatsCompliant();
                Table t;
                if (isInvalidTxnStats) {
                    firstColStats.setStatsObj(Lists.newArrayList());
                } else {
                    t = this.getTable(catName, dbName, tableName);
                    MetaStoreUtils.getMergableCols(firstColStats, t.getParameters());
                    if (csOld != null && csOld.getStatsObjSize() != 0 && !firstColStats.getStatsObj().isEmpty()) {
                        MetaStoreUtils.mergeColStats(firstColStats, csOld);
                    }
                }

                if (!firstColStats.getStatsObj().isEmpty()) {
                    result = this.updateTableColumnStatsInternal(firstColStats, request.getValidWriteIdList(), request.getWriteId());
                } else if (isInvalidTxnStats) {
                    t = this.getTable(catName, dbName, tableName);
                    t.setWriteId(request.getWriteId());
                    StatsSetupConst.clearColumnStatsState(t.getParameters());
                    StatsSetupConst.setBasicStatsState(t.getParameters(), "false");
                    ms.alterTable(catName, dbName, tableName, t, request.getValidWriteIdList());
                } else {
                    LOG.debug("All the column stats are not accurate to merge.");
                    result = true;
                }

                ms.commitTransaction();
                isCommitted = true;
            } finally {
                if (!isCommitted) {
                    ms.rollbackTransaction();
                }

            }

            return result;
        }

        private Table getTable(String catName, String dbName, String tableName) throws MetaException, InvalidObjectException {
            return this.getTable(catName, dbName, tableName, (String)null);
        }

        private Table getTable(String catName, String dbName, String tableName, String writeIdList) throws MetaException, InvalidObjectException {
            Table t = this.getMS().getTable(catName, dbName, tableName, writeIdList);
            if (t == null) {
                throw new InvalidObjectException(Warehouse.getCatalogQualifiedTableName(catName, dbName, tableName) + " table not found");
            } else {
                return t;
            }
        }

        public NotificationEventResponse get_next_notification(NotificationEventRequest rqst) throws TException {
            try {
                this.authorizeProxyPrivilege();
            } catch (Exception var3) {
                LOG.error("Not authorized to make the get_next_notification call. You can try to disable " + ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.toString(), var3);
                throw new TException(var3);
            }

            RawStore ms = this.getMS();
            return ms.getNextNotification(rqst);
        }

        public CurrentNotificationEventId get_current_notificationEventId() throws TException {
            try {
                this.authorizeProxyPrivilege();
            } catch (Exception var2) {
                LOG.error("Not authorized to make the get_current_notificationEventId call. You can try to disable " + ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.toString(), var2);
                throw new TException(var2);
            }

            RawStore ms = this.getMS();
            return ms.getCurrentNotificationEventId();
        }

        public NotificationEventsCountResponse get_notification_events_count(NotificationEventsCountRequest rqst) throws TException {
            try {
                this.authorizeProxyPrivilege();
            } catch (Exception var3) {
                LOG.error("Not authorized to make the get_notification_events_count call. You can try to disable " + ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.toString(), var3);
                throw new TException(var3);
            }

            RawStore ms = this.getMS();
            return ms.getNotificationEventsCount(rqst);
        }

        private void authorizeProxyPrivilege() throws Exception {
            if (HiveMetaStore.isMetaStoreRemote() && MetastoreConf.getBoolVar(this.conf, ConfVars.EVENT_DB_NOTIFICATION_API_AUTH)) {
                String user = null;

                try {
                    user = SecurityUtils.getUGI().getShortUserName();
                } catch (Exception var3) {
                    LOG.error("Cannot obtain username", var3);
                    throw var3;
                }

                if (!MetaStoreUtils.checkUserHasHostProxyPrivileges(user, this.conf, getIPAddress())) {
                    throw new MetaException("User " + user + " is not allowed to perform this API call");
                }
            }
        }

        public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
            switch((_Fields)rqst.getData().getSetField()) {
                case INSERT_DATA:
                    String catName = rqst.isSetCatName() ? rqst.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
                    InsertEvent event = new InsertEvent(catName, rqst.getDbName(), rqst.getTableName(), rqst.getPartitionVals(), rqst.getData().getInsertData(), rqst.isSuccessful(), this);
                    MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.INSERT, event);
                    MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.INSERT, event);
                    return new FireEventResponse();
                default:
                    throw new TException("Event type " + ((_Fields)rqst.getData().getSetField()).toString() + " not currently supported.");
            }
        }

        public GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest req) throws TException {
            GetFileMetadataByExprResult result = new GetFileMetadataByExprResult();
            RawStore ms = this.getMS();
            if (!ms.isFileMetadataSupported()) {
                result.setIsSupported(false);
                result.setMetadata(EMPTY_MAP_FM2);
                return result;
            } else {
                result.setIsSupported(true);
                List<Long> fileIds = req.getFileIds();
                boolean needMetadata = !req.isSetDoGetFooters() || req.isDoGetFooters();
                FileMetadataExprType type = req.isSetType() ? req.getType() : FileMetadataExprType.ORC_SARG;
                ByteBuffer[] metadatas = needMetadata ? new ByteBuffer[fileIds.size()] : null;
                ByteBuffer[] ppdResults = new ByteBuffer[fileIds.size()];
                boolean[] eliminated = new boolean[fileIds.size()];
                this.getMS().getFileMetadataByExpr(fileIds, type, req.getExpr(), metadatas, ppdResults, eliminated);

                for(int i = 0; i < fileIds.size(); ++i) {
                    if (eliminated[i] || ppdResults[i] != null) {
                        MetadataPpdResult mpr = new MetadataPpdResult();
                        ByteBuffer ppdResult = eliminated[i] ? null : this.handleReadOnlyBufferForThrift(ppdResults[i]);
                        mpr.setIncludeBitset(ppdResult);
                        if (needMetadata) {
                            ByteBuffer metadata = eliminated[i] ? null : this.handleReadOnlyBufferForThrift(metadatas[i]);
                            mpr.setMetadata(metadata);
                        }

                        result.putToMetadata((Long)fileIds.get(i), mpr);
                    }
                }

                if (!result.isSetMetadata()) {
                    result.setMetadata(EMPTY_MAP_FM2);
                }

                return result;
            }
        }

        public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
            GetFileMetadataResult result = new GetFileMetadataResult();
            RawStore ms = this.getMS();
            if (!ms.isFileMetadataSupported()) {
                result.setIsSupported(false);
                result.setMetadata(EMPTY_MAP_FM1);
                return result;
            } else {
                result.setIsSupported(true);
                List<Long> fileIds = req.getFileIds();
                ByteBuffer[] metadatas = ms.getFileMetadata(fileIds);

                assert metadatas.length == fileIds.size();

                for(int i = 0; i < metadatas.length; ++i) {
                    ByteBuffer bb = metadatas[i];
                    if (bb != null) {
                        bb = this.handleReadOnlyBufferForThrift(bb);
                        result.putToMetadata((Long)fileIds.get(i), bb);
                    }
                }

                if (!result.isSetMetadata()) {
                    result.setMetadata(EMPTY_MAP_FM1);
                }

                return result;
            }
        }

        private ByteBuffer handleReadOnlyBufferForThrift(ByteBuffer bb) {
            if (!bb.isReadOnly()) {
                return bb;
            } else {
                ByteBuffer copy = ByteBuffer.allocate(bb.capacity());
                copy.put(bb);
                copy.flip();
                return copy;
            }
        }

        public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest req) throws TException {
            RawStore ms = this.getMS();
            if (ms.isFileMetadataSupported()) {
                ms.putFileMetadata(req.getFileIds(), req.getMetadata(), req.getType());
            }

            return new PutFileMetadataResult();
        }

        public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req) throws TException {
            this.getMS().putFileMetadata(req.getFileIds(), (List)null, (FileMetadataExprType)null);
            return new ClearFileMetadataResult();
        }

        public CacheFileMetadataResult cache_file_metadata(CacheFileMetadataRequest req) throws TException {
            RawStore ms = this.getMS();
            if (!ms.isFileMetadataSupported()) {
                return new CacheFileMetadataResult(false);
            } else {
                String dbName = req.getDbName();
                String tblName = req.getTblName();
                String partName = req.isSetPartName() ? req.getPartName() : null;
                boolean isAllPart = req.isSetIsAllParts() && req.isIsAllParts();
                ms.openTransaction();
                boolean success = false;

                try {
                    Table tbl = ms.getTable("hive", dbName, tblName);
                    if (tbl == null) {
                        throw new NoSuchObjectException(dbName + "." + tblName + " not found");
                    } else {
                        boolean isPartitioned = tbl.isSetPartitionKeys() && tbl.getPartitionKeysSize() > 0;
                        String tableInputFormat = tbl.isSetSd() ? tbl.getSd().getInputFormat() : null;
                        if (!isPartitioned) {
                            if (partName == null && !isAllPart) {
                                if (tbl.isSetSd() && tbl.getSd().isSetLocation()) {
                                    FileMetadataExprType type = this.expressionProxy.getMetadataType(tableInputFormat);
                                    if (type == null) {
                                        throw new MetaException("The operation is not supported for " + tableInputFormat);
                                    } else {
                                        this.fileMetadataManager.queueCacheMetadata(tbl.getSd().getLocation(), type);
                                        success = true;
                                        return new CacheFileMetadataResult(true);
                                    }
                                } else {
                                    throw new MetaException("Table does not have storage location; this operation is not supported on views");
                                }
                            } else {
                                throw new MetaException("Table is not partitioned");
                            }
                        } else {
                            Object partNames;
                            if (partName != null) {
                                partNames = Lists.newArrayList(new String[]{partName});
                            } else {
                                if (!isAllPart) {
                                    throw new MetaException("Table is partitioned");
                                }

                                partNames = ms.listPartitionNames("hive", dbName, tblName, (short)-1);
                            }

                            int batchSize = MetastoreConf.getIntVar(this.conf, ConfVars.BATCH_RETRIEVE_OBJECTS_MAX);
                            int index = 0;
                            int successCount = 0;
                            int failCount = 0;
                            HashSet failFormats = null;

                            while(index < ((List)partNames).size()) {
                                int currentBatchSize = Math.min(batchSize, ((List)partNames).size() - index);
                                List<String> nameBatch = ((List)partNames).subList(index, index + currentBatchSize);
                                index += currentBatchSize;
                                List<Partition> parts = ms.getPartitionsByNames("hive", dbName, tblName, nameBatch);
                                Iterator var20 = parts.iterator();

                                while(var20.hasNext()) {
                                    Partition part = (Partition)var20.next();
                                    if (!part.isSetSd() || !part.getSd().isSetLocation()) {
                                        throw new MetaException("Partition does not have storage location; this operation is not supported on views");
                                    }

                                    String inputFormat = part.getSd().isSetInputFormat() ? part.getSd().getInputFormat() : tableInputFormat;
                                    FileMetadataExprType type = this.expressionProxy.getMetadataType(inputFormat);
                                    if (type == null) {
                                        ++failCount;
                                        if (failFormats == null) {
                                            failFormats = new HashSet();
                                        }

                                        failFormats.add(inputFormat);
                                    } else {
                                        ++successCount;
                                        this.fileMetadataManager.queueCacheMetadata(part.getSd().getLocation(), type);
                                    }
                                }
                            }

                            success = true;
                            if (failCount > 0) {
                                String errorMsg = "The operation failed for " + failCount + " partitions and " + "succeeded for " + successCount + " partitions; unsupported formats: ";
                                boolean isFirst = true;

                                String s;
                                for(Iterator var30 = failFormats.iterator(); var30.hasNext(); errorMsg = errorMsg + s) {
                                    s = (String)var30.next();
                                    if (!isFirst) {
                                        errorMsg = errorMsg + ", ";
                                    }

                                    isFirst = false;
                                }

                                throw new MetaException(errorMsg);
                            } else {
                                return new CacheFileMetadataResult(true);
                            }
                        }
                    }
                } finally {
                    if (success) {
                        if (!ms.commitTransaction()) {
                            throw new MetaException("Failed to commit");
                        }
                    } else {
                        ms.rollbackTransaction();
                    }

                }
            }
        }

        @VisibleForTesting
        void updateMetrics() throws MetaException {
            if (databaseCount != null) {
                tableCount.set(this.getMS().getTableCount());
                partCount.set(this.getMS().getPartitionCount());
                databaseCount.set(this.getMS().getDatabaseCount());
            }

        }

        public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request) throws TException {
            String catName = request.isSetCatName() ? request.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String db_name = request.getDb_name();
            String tbl_name = request.getTbl_name();
            this.startTableFunction("get_primary_keys", catName, db_name, tbl_name);
            List<SQLPrimaryKey> ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().getPrimaryKeys(catName, db_name, tbl_name);
            } catch (Exception var11) {
                ex = var11;
                this.throwMetaException(var11);
            } finally {
                this.endFunction("get_primary_keys", ret != null, ex, tbl_name);
            }

            return new PrimaryKeysResponse(ret);
        }

        public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request) throws TException {
            String catName = request.isSetCatName() ? request.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String parent_db_name = request.getParent_db_name();
            String parent_tbl_name = request.getParent_tbl_name();
            String foreign_db_name = request.getForeign_db_name();
            String foreign_tbl_name = request.getForeign_tbl_name();
            this.startFunction("get_foreign_keys", " : parentdb=" + parent_db_name + " parenttbl=" + parent_tbl_name + " foreigndb=" + foreign_db_name + " foreigntbl=" + foreign_tbl_name);
            List<SQLForeignKey> ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().getForeignKeys(catName, parent_db_name, parent_tbl_name, foreign_db_name, foreign_tbl_name);
            } catch (Exception var13) {
                ex = var13;
                this.throwMetaException(var13);
            } finally {
                this.endFunction("get_foreign_keys", ret != null, ex, foreign_tbl_name);
            }

            return new ForeignKeysResponse(ret);
        }

        private void throwMetaException(Exception e) throws MetaException, NoSuchObjectException {
            if (e instanceof MetaException) {
                throw (MetaException)e;
            } else if (e instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e;
            } else {
                throw newMetaException(e);
            }
        }

        public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest request) throws TException {
            String catName = request.isSetCatName() ? request.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String db_name = request.getDb_name();
            String tbl_name = request.getTbl_name();
            this.startTableFunction("get_unique_constraints", catName, db_name, tbl_name);
            List<SQLUniqueConstraint> ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().getUniqueConstraints(catName, db_name, tbl_name);
            } catch (Exception var11) {
                ex = var11;
                if (var11 instanceof MetaException) {
                    throw (MetaException)var11;
                }

                throw newMetaException(var11);
            } finally {
                this.endFunction("get_unique_constraints", ret != null, ex, tbl_name);
            }

            return new UniqueConstraintsResponse(ret);
        }

        public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest request) throws TException {
            String catName = request.isSetCatName() ? request.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String db_name = request.getDb_name();
            String tbl_name = request.getTbl_name();
            this.startTableFunction("get_not_null_constraints", catName, db_name, tbl_name);
            List<SQLNotNullConstraint> ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().getNotNullConstraints(catName, db_name, tbl_name);
            } catch (Exception var11) {
                ex = var11;
                if (var11 instanceof MetaException) {
                    throw (MetaException)var11;
                }

                throw newMetaException(var11);
            } finally {
                this.endFunction("get_not_null_constraints", ret != null, ex, tbl_name);
            }

            return new NotNullConstraintsResponse(ret);
        }

        public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest request) throws TException {
            String catName = request.isSetCatName() ? request.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf);
            String db_name = request.getDb_name();
            String tbl_name = request.getTbl_name();
            this.startTableFunction("get_default_constraints", catName, db_name, tbl_name);
            List<SQLDefaultConstraint> ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().getDefaultConstraints(catName, db_name, tbl_name);
            } catch (Exception var11) {
                ex = var11;
                if (var11 instanceof MetaException) {
                    throw (MetaException)var11;
                }

                throw newMetaException(var11);
            } finally {
                this.endFunction("get_default_constraints", ret != null, ex, tbl_name);
            }

            return new DefaultConstraintsResponse(ret);
        }

        public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest request) throws TException {
            String catName = request.getCatName();
            String db_name = request.getDb_name();
            String tbl_name = request.getTbl_name();
            this.startTableFunction("get_check_constraints", catName, db_name, tbl_name);
            List<SQLCheckConstraint> ret = null;
            Exception ex = null;

            try {
                ret = this.getMS().getCheckConstraints(catName, db_name, tbl_name);
            } catch (Exception var11) {
                ex = var11;
                if (var11 instanceof MetaException) {
                    throw (MetaException)var11;
                }

                throw newMetaException(var11);
            } finally {
                this.endFunction("get_check_constraints", ret != null, ex, tbl_name);
            }

            return new CheckConstraintsResponse(ret);
        }

        public String get_metastore_db_uuid() throws TException {
            try {
                return this.getMS().getMetastoreDbUuid();
            } catch (MetaException var2) {
                LOG.error("Exception thrown while querying metastore db uuid", var2);
                throw var2;
            }
        }

        public WMCreateResourcePlanResponse create_resource_plan(WMCreateResourcePlanRequest request) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
            int defaultPoolSize = MetastoreConf.getIntVar(this.conf, ConfVars.WM_DEFAULT_POOL_SIZE);
            WMResourcePlan plan = request.getResourcePlan();
            if (defaultPoolSize > 0 && plan.isSetQueryParallelism()) {
                defaultPoolSize = plan.getQueryParallelism();
            }

            try {
                this.getMS().createResourcePlan(plan, request.getCopyFrom(), defaultPoolSize);
                return new WMCreateResourcePlanResponse();
            } catch (MetaException var5) {
                LOG.error("Exception while trying to persist resource plan", var5);
                throw var5;
            }
        }

        public WMGetResourcePlanResponse get_resource_plan(WMGetResourcePlanRequest request) throws NoSuchObjectException, MetaException, TException {
            try {
                WMFullResourcePlan rp = this.getMS().getResourcePlan(request.getResourcePlanName());
                WMGetResourcePlanResponse resp = new WMGetResourcePlanResponse();
                resp.setResourcePlan(rp);
                return resp;
            } catch (MetaException var4) {
                LOG.error("Exception while trying to retrieve resource plan", var4);
                throw var4;
            }
        }

        public WMGetAllResourcePlanResponse get_all_resource_plans(WMGetAllResourcePlanRequest request) throws MetaException, TException {
            try {
                WMGetAllResourcePlanResponse resp = new WMGetAllResourcePlanResponse();
                resp.setResourcePlans(this.getMS().getAllResourcePlans());
                return resp;
            } catch (MetaException var3) {
                LOG.error("Exception while trying to retrieve resource plans", var3);
                throw var3;
            }
        }

        public WMAlterResourcePlanResponse alter_resource_plan(WMAlterResourcePlanRequest request) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
            try {
                if ((request.isIsEnableAndActivate() ? 1 : 0) + (request.isIsReplace() ? 1 : 0) + (request.isIsForceDeactivate() ? 1 : 0) > 1) {
                    throw new MetaException("Invalid request; multiple flags are set");
                } else {
                    WMAlterResourcePlanResponse response = new WMAlterResourcePlanResponse();
                    WMFullResourcePlan fullPlanAfterAlter = this.getMS().alterResourcePlan(request.getResourcePlanName(), request.getResourcePlan(), request.isIsEnableAndActivate(), request.isIsForceDeactivate(), request.isIsReplace());
                    if (fullPlanAfterAlter != null) {
                        response.setFullResourcePlan(fullPlanAfterAlter);
                    }

                    return response;
                }
            } catch (MetaException var4) {
                LOG.error("Exception while trying to alter resource plan", var4);
                throw var4;
            }
        }

        public WMGetActiveResourcePlanResponse get_active_resource_plan(WMGetActiveResourcePlanRequest request) throws MetaException, TException {
            try {
                WMGetActiveResourcePlanResponse response = new WMGetActiveResourcePlanResponse();
                response.setResourcePlan(this.getMS().getActiveResourcePlan());
                return response;
            } catch (MetaException var3) {
                LOG.error("Exception while trying to get active resource plan", var3);
                throw var3;
            }
        }

        public WMValidateResourcePlanResponse validate_resource_plan(WMValidateResourcePlanRequest request) throws NoSuchObjectException, MetaException, TException {
            try {
                return this.getMS().validateResourcePlan(request.getResourcePlanName());
            } catch (MetaException var3) {
                LOG.error("Exception while trying to validate resource plan", var3);
                throw var3;
            }
        }

        public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest request) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
            try {
                this.getMS().dropResourcePlan(request.getResourcePlanName());
                return new WMDropResourcePlanResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to drop resource plan", var3);
                throw var3;
            }
        }

        public WMCreateTriggerResponse create_wm_trigger(WMCreateTriggerRequest request) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
            try {
                this.getMS().createWMTrigger(request.getTrigger());
                return new WMCreateTriggerResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to create trigger", var3);
                throw var3;
            }
        }

        public WMAlterTriggerResponse alter_wm_trigger(WMAlterTriggerRequest request) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
            try {
                this.getMS().alterWMTrigger(request.getTrigger());
                return new WMAlterTriggerResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to alter trigger", var3);
                throw var3;
            }
        }

        public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest request) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
            try {
                this.getMS().dropWMTrigger(request.getResourcePlanName(), request.getTriggerName());
                return new WMDropTriggerResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to drop trigger.", var3);
                throw var3;
            }
        }

        public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(WMGetTriggersForResourePlanRequest request) throws NoSuchObjectException, MetaException, TException {
            try {
                List<WMTrigger> triggers = this.getMS().getTriggersForResourcePlan(request.getResourcePlanName());
                WMGetTriggersForResourePlanResponse response = new WMGetTriggersForResourePlanResponse();
                response.setTriggers(triggers);
                return response;
            } catch (MetaException var4) {
                LOG.error("Exception while trying to retrieve triggers plans", var4);
                throw var4;
            }
        }

        public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest request) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
            try {
                this.getMS().alterPool(request.getPool(), request.getPoolPath());
                return new WMAlterPoolResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to alter WMPool", var3);
                throw var3;
            }
        }

        public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest request) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
            try {
                this.getMS().createPool(request.getPool());
                return new WMCreatePoolResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to create WMPool", var3);
                throw var3;
            }
        }

        public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest request) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
            try {
                this.getMS().dropWMPool(request.getResourcePlanName(), request.getPoolPath());
                return new WMDropPoolResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to drop WMPool", var3);
                throw var3;
            }
        }

        public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(WMCreateOrUpdateMappingRequest request) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
            try {
                this.getMS().createOrUpdateWMMapping(request.getMapping(), request.isUpdate());
                return new WMCreateOrUpdateMappingResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to create or update WMMapping", var3);
                throw var3;
            }
        }

        public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest request) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
            try {
                this.getMS().dropWMMapping(request.getMapping());
                return new WMDropMappingResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to drop WMMapping", var3);
                throw var3;
            }
        }

        public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(WMCreateOrDropTriggerToPoolMappingRequest request) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
            try {
                if (request.isDrop()) {
                    this.getMS().dropWMTriggerToPoolMapping(request.getResourcePlanName(), request.getTriggerName(), request.getPoolPath());
                } else {
                    this.getMS().createWMTriggerToPoolMapping(request.getResourcePlanName(), request.getTriggerName(), request.getPoolPath());
                }

                return new WMCreateOrDropTriggerToPoolMappingResponse();
            } catch (MetaException var3) {
                LOG.error("Exception while trying to create or drop pool mappings", var3);
                throw var3;
            }
        }

        public void create_ischema(ISchema schema) throws TException {
            this.startFunction("create_ischema", ": " + schema.getName());
            boolean success = false;
            Exception ex = null;
            RawStore ms = this.getMS();

            try {
                this.firePreEvent(new PreCreateISchemaEvent(this, schema));
                Map<String, String> transactionalListenersResponses = Collections.emptyMap();
                ms.openTransaction();

                try {
                    ms.createISchema(schema);
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.CREATE_ISCHEMA, new CreateISchemaEvent(true, this, schema));
                    }

                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.CREATE_ISCHEMA, new CreateISchemaEvent(success, this, schema), (EnvironmentContext)null, transactionalListenersResponses, ms);
                    }

                }
            } catch (AlreadyExistsException | MetaException var15) {
                LOG.error("Caught exception creating schema", var15);
                ex = var15;
                throw var15;
            } finally {
                this.endFunction("create_ischema", success, ex);
            }

        }

        public void alter_ischema(AlterISchemaRequest rqst) throws TException {
            this.startFunction("alter_ischema", ": " + rqst);
            boolean success = false;
            Exception ex = null;
            RawStore ms = this.getMS();

            try {
                ISchema oldSchema = ms.getISchema(rqst.getName());
                if (oldSchema == null) {
                    throw new NoSuchObjectException("Could not find schema " + rqst.getName());
                }

                this.firePreEvent(new PreAlterISchemaEvent(this, oldSchema, rqst.getNewSchema()));
                Map<String, String> transactionalListenersResponses = Collections.emptyMap();
                ms.openTransaction();

                try {
                    ms.alterISchema(rqst.getName(), rqst.getNewSchema());
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ALTER_ISCHEMA, new AlterISchemaEvent(true, this, oldSchema, rqst.getNewSchema()));
                    }

                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALTER_ISCHEMA, new AlterISchemaEvent(success, this, oldSchema, rqst.getNewSchema()), (EnvironmentContext)null, transactionalListenersResponses, ms);
                    }

                }
            } catch (NoSuchObjectException | MetaException var16) {
                LOG.error("Caught exception altering schema", var16);
                ex = var16;
                throw var16;
            } finally {
                this.endFunction("alter_ischema", success, ex);
            }

        }

        public ISchema get_ischema(ISchemaName schemaName) throws TException {
            this.startFunction("get_ischema", ": " + schemaName);
            Exception ex = null;
            ISchema schema = null;

            ISchema var4;
            try {
                schema = this.getMS().getISchema(schemaName);
                if (schema == null) {
                    throw new NoSuchObjectException("No schema named " + schemaName + " exists");
                }

                this.firePreEvent(new PreReadISchemaEvent(this, schema));
                var4 = schema;
            } catch (MetaException var8) {
                LOG.error("Caught exception getting schema", var8);
                ex = var8;
                throw var8;
            } finally {
                this.endFunction("get_ischema", schema != null, ex);
            }

            return var4;
        }

        public void drop_ischema(ISchemaName schemaName) throws TException {
            this.startFunction("drop_ischema", ": " + schemaName);
            Exception ex = null;
            boolean success = false;
            RawStore ms = this.getMS();

            try {
                SchemaVersion latest = ms.getLatestSchemaVersion(schemaName);
                if (latest != null) {
                    ex = new InvalidOperationException("Schema " + schemaName + " cannot be dropped, it has" + " at least one valid version");
                    throw (InvalidObjectException)ex;
                }

                ISchema schema = ms.getISchema(schemaName);
                this.firePreEvent(new PreDropISchemaEvent(this, schema));
                Map<String, String> transactionalListenersResponses = Collections.emptyMap();
                ms.openTransaction();

                try {
                    ms.dropISchema(schemaName);
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.DROP_ISCHEMA, new DropISchemaEvent(true, this, schema));
                    }

                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.DROP_ISCHEMA, new DropISchemaEvent(success, this, schema), (EnvironmentContext)null, transactionalListenersResponses, ms);
                    }

                }
            } catch (NoSuchObjectException | MetaException var17) {
                LOG.error("Caught exception dropping schema", var17);
                ex = var17;
                throw var17;
            } finally {
                this.endFunction("drop_ischema", success, (Exception)ex);
            }

        }

        public void add_schema_version(SchemaVersion schemaVersion) throws TException {
            this.startFunction("add_schema_version", ": " + schemaVersion);
            boolean success = false;
            Exception ex = null;
            RawStore ms = this.getMS();

            try {
                if (ms.getISchema(schemaVersion.getSchema()) == null) {
                    throw new NoSuchObjectException("No schema named " + schemaVersion.getSchema());
                }

                this.firePreEvent(new PreAddSchemaVersionEvent(this, schemaVersion));
                Map<String, String> transactionalListenersResponses = Collections.emptyMap();
                ms.openTransaction();

                try {
                    ms.addSchemaVersion(schemaVersion);
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ADD_SCHEMA_VERSION, new AddSchemaVersionEvent(true, this, schemaVersion));
                    }

                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ADD_SCHEMA_VERSION, new AddSchemaVersionEvent(success, this, schemaVersion), (EnvironmentContext)null, transactionalListenersResponses, ms);
                    }

                }
            } catch (AlreadyExistsException | MetaException var15) {
                LOG.error("Caught exception adding schema version", var15);
                ex = var15;
                throw var15;
            } finally {
                this.endFunction("add_schema_version", success, ex);
            }

        }

        public SchemaVersion get_schema_version(SchemaVersionDescriptor version) throws TException {
            this.startFunction("get_schema_version", ": " + version);
            Exception ex = null;
            SchemaVersion schemaVersion = null;

            SchemaVersion var4;
            try {
                schemaVersion = this.getMS().getSchemaVersion(version);
                if (schemaVersion == null) {
                    throw new NoSuchObjectException("No schema version " + version + "exists");
                }

                this.firePreEvent(new PreReadhSchemaVersionEvent(this, Collections.singletonList(schemaVersion)));
                var4 = schemaVersion;
            } catch (MetaException var8) {
                LOG.error("Caught exception getting schema version", var8);
                ex = var8;
                throw var8;
            } finally {
                this.endFunction("get_schema_version", schemaVersion != null, ex);
            }

            return var4;
        }

        public SchemaVersion get_schema_latest_version(ISchemaName schemaName) throws TException {
            this.startFunction("get_latest_schema_version", ": " + schemaName);
            Exception ex = null;
            SchemaVersion schemaVersion = null;

            SchemaVersion var4;
            try {
                schemaVersion = this.getMS().getLatestSchemaVersion(schemaName);
                if (schemaVersion == null) {
                    throw new NoSuchObjectException("No versions of schema " + schemaName + "exist");
                }

                this.firePreEvent(new PreReadhSchemaVersionEvent(this, Collections.singletonList(schemaVersion)));
                var4 = schemaVersion;
            } catch (MetaException var8) {
                LOG.error("Caught exception getting latest schema version", var8);
                ex = var8;
                throw var8;
            } finally {
                this.endFunction("get_latest_schema_version", schemaVersion != null, ex);
            }

            return var4;
        }

        public List<SchemaVersion> get_schema_all_versions(ISchemaName schemaName) throws TException {
            this.startFunction("get_all_schema_versions", ": " + schemaName);
            Exception ex = null;
            List schemaVersions = null;

            List var4;
            try {
                schemaVersions = this.getMS().getAllSchemaVersion(schemaName);
                if (schemaVersions == null) {
                    throw new NoSuchObjectException("No versions of schema " + schemaName + "exist");
                }

                this.firePreEvent(new PreReadhSchemaVersionEvent(this, schemaVersions));
                var4 = schemaVersions;
            } catch (MetaException var8) {
                LOG.error("Caught exception getting all schema versions", var8);
                ex = var8;
                throw var8;
            } finally {
                this.endFunction("get_all_schema_versions", schemaVersions != null, ex);
            }

            return var4;
        }

        public void drop_schema_version(SchemaVersionDescriptor version) throws TException {
            this.startFunction("drop_schema_version", ": " + version);
            Exception ex = null;
            boolean success = false;
            RawStore ms = this.getMS();

            try {
                SchemaVersion schemaVersion = ms.getSchemaVersion(version);
                if (schemaVersion == null) {
                    throw new NoSuchObjectException("No schema version " + version);
                }

                this.firePreEvent(new PreDropSchemaVersionEvent(this, schemaVersion));
                Map<String, String> transactionalListenersResponses = Collections.emptyMap();
                ms.openTransaction();

                try {
                    ms.dropSchemaVersion(version);
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.DROP_SCHEMA_VERSION, new DropSchemaVersionEvent(true, this, schemaVersion));
                    }

                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.DROP_SCHEMA_VERSION, new DropSchemaVersionEvent(success, this, schemaVersion), (EnvironmentContext)null, transactionalListenersResponses, ms);
                    }

                }
            } catch (NoSuchObjectException | MetaException var16) {
                LOG.error("Caught exception dropping schema version", var16);
                ex = var16;
                throw var16;
            } finally {
                this.endFunction("drop_schema_version", success, ex);
            }

        }

        public FindSchemasByColsResp get_schemas_by_cols(FindSchemasByColsRqst rqst) throws TException {
            this.startFunction("get_schemas_by_cols");
            Exception ex = null;
            List<SchemaVersion> schemaVersions = Collections.emptyList();

            FindSchemasByColsResp var5;
            try {
                schemaVersions = this.getMS().getSchemaVersionsByColumns(rqst.getColName(), rqst.getColNamespace(), rqst.getType());
                this.firePreEvent(new PreReadhSchemaVersionEvent(this, schemaVersions));
                List<SchemaVersionDescriptor> entries = new ArrayList(schemaVersions.size());
                schemaVersions.forEach((schemaVersion) -> {
                    entries.add(new SchemaVersionDescriptor(schemaVersion.getSchema(), schemaVersion.getVersion()));
                });
                var5 = new FindSchemasByColsResp(entries);
            } catch (MetaException var9) {
                LOG.error("Caught exception doing schema version query", var9);
                ex = var9;
                throw var9;
            } finally {
                this.endFunction("get_schemas_by_cols", !schemaVersions.isEmpty(), ex);
            }

            return var5;
        }

        public void map_schema_version_to_serde(MapSchemaVersionToSerdeRequest rqst) throws TException {
            this.startFunction("map_schema_version_to_serde, :" + rqst);
            boolean success = false;
            Exception ex = null;
            RawStore ms = this.getMS();

            try {
                SchemaVersion oldSchemaVersion = ms.getSchemaVersion(rqst.getSchemaVersion());
                if (oldSchemaVersion == null) {
                    throw new NoSuchObjectException("No schema version " + rqst.getSchemaVersion());
                }

                SerDeInfo serde = ms.getSerDeInfo(rqst.getSerdeName());
                if (serde == null) {
                    throw new NoSuchObjectException("No SerDe named " + rqst.getSerdeName());
                }

                SchemaVersion newSchemaVersion = new SchemaVersion(oldSchemaVersion);
                newSchemaVersion.setSerDe(serde);
                this.firePreEvent(new PreAlterSchemaVersionEvent(this, oldSchemaVersion, newSchemaVersion));
                Map<String, String> transactionalListenersResponses = Collections.emptyMap();
                ms.openTransaction();

                try {
                    ms.alterSchemaVersion(rqst.getSchemaVersion(), newSchemaVersion);
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ALTER_SCHEMA_VERSION, new AlterSchemaVersionEvent(true, this, oldSchemaVersion, newSchemaVersion));
                    }

                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALTER_SCHEMA_VERSION, new AlterSchemaVersionEvent(success, this, oldSchemaVersion, newSchemaVersion), (EnvironmentContext)null, transactionalListenersResponses, ms);
                    }

                }
            } catch (NoSuchObjectException | MetaException var18) {
                LOG.error("Caught exception mapping schema version to serde", var18);
                ex = var18;
                throw var18;
            } finally {
                this.endFunction("map_schema_version_to_serde", success, ex);
            }

        }

        public void set_schema_version_state(SetSchemaVersionStateRequest rqst) throws TException {
            this.startFunction("set_schema_version_state, :" + rqst);
            boolean success = false;
            Exception ex = null;
            RawStore ms = this.getMS();

            try {
                SchemaVersion oldSchemaVersion = ms.getSchemaVersion(rqst.getSchemaVersion());
                if (oldSchemaVersion == null) {
                    throw new NoSuchObjectException("No schema version " + rqst.getSchemaVersion());
                }

                SchemaVersion newSchemaVersion = new SchemaVersion(oldSchemaVersion);
                newSchemaVersion.setState(rqst.getState());
                this.firePreEvent(new PreAlterSchemaVersionEvent(this, oldSchemaVersion, newSchemaVersion));
                Map<String, String> transactionalListenersResponses = Collections.emptyMap();
                ms.openTransaction();

                try {
                    ms.alterSchemaVersion(rqst.getSchemaVersion(), newSchemaVersion);
                    if (!this.transactionalListeners.isEmpty()) {
                        transactionalListenersResponses = MetaStoreListenerNotifier.notifyEvent(this.transactionalListeners, EventType.ALTER_SCHEMA_VERSION, new AlterSchemaVersionEvent(true, this, oldSchemaVersion, newSchemaVersion));
                    }

                    success = ms.commitTransaction();
                } finally {
                    if (!success) {
                        ms.rollbackTransaction();
                    }

                    if (!this.listeners.isEmpty()) {
                        MetaStoreListenerNotifier.notifyEvent(this.listeners, EventType.ALTER_SCHEMA_VERSION, new AlterSchemaVersionEvent(success, this, oldSchemaVersion, newSchemaVersion), (EnvironmentContext)null, transactionalListenersResponses, ms);
                    }

                }
            } catch (NoSuchObjectException | MetaException var17) {
                LOG.error("Caught exception changing schema version state", var17);
                ex = var17;
                throw var17;
            } finally {
                this.endFunction("set_schema_version_state", success, ex);
            }

        }

        public void add_serde(SerDeInfo serde) throws TException {
            this.startFunction("create_serde", ": " + serde.getName());
            Exception ex = null;
            boolean success = false;
            RawStore ms = this.getMS();

            try {
                ms.openTransaction();
                ms.addSerde(serde);
                success = ms.commitTransaction();
            } catch (AlreadyExistsException | MetaException var9) {
                LOG.error("Caught exception creating serde", var9);
                ex = var9;
                throw var9;
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                }

                this.endFunction("create_serde", success, ex);
            }

        }

        public SerDeInfo get_serde(GetSerdeRequest rqst) throws TException {
            this.startFunction("get_serde", ": " + rqst);
            Exception ex = null;
            SerDeInfo serde = null;

            SerDeInfo var4;
            try {
                serde = this.getMS().getSerDeInfo(rqst.getSerdeName());
                if (serde == null) {
                    throw new NoSuchObjectException("No serde named " + rqst.getSerdeName() + " exists");
                }

                var4 = serde;
            } catch (MetaException var8) {
                LOG.error("Caught exception getting serde", var8);
                ex = var8;
                throw var8;
            } finally {
                this.endFunction("get_serde", serde != null, ex);
            }

            return var4;
        }

        public LockResponse get_lock_materialization_rebuild(String dbName, String tableName, long txnId) throws TException {
            return this.getTxnHandler().lockMaterializationRebuild(dbName, tableName, txnId);
        }

        public boolean heartbeat_lock_materialization_rebuild(String dbName, String tableName, long txnId) throws TException {
            return this.getTxnHandler().heartbeatLockMaterializationRebuild(dbName, tableName, txnId);
        }

        public void add_runtime_stats(RuntimeStat stat) throws TException {
            this.startFunction("store_runtime_stats");
            Exception ex = null;
            boolean success = false;
            RawStore ms = this.getMS();

            try {
                ms.openTransaction();
                ms.addRuntimeStat(stat);
                success = ms.commitTransaction();
            } catch (Exception var9) {
                LOG.error("Caught exception", var9);
                ex = var9;
                throw var9;
            } finally {
                if (!success) {
                    ms.rollbackTransaction();
                }

                this.endFunction("store_runtime_stats", success, ex);
            }

        }

        public List<RuntimeStat> get_runtime_stats(GetRuntimeStatsRequest rqst) throws TException {
            this.startFunction("get_runtime_stats");
            MetaException ex = null;

            List var4;
            try {
                List<RuntimeStat> res = this.getMS().getRuntimeStats(rqst.getMaxWeight(), rqst.getMaxCreateTime());
                var4 = res;
            } catch (MetaException var8) {
                LOG.error("Caught exception", var8);
                ex = var8;
                throw var8;
            } finally {
                this.endFunction("get_runtime_stats", ex == null, ex);
            }

            return var4;
        }

        static {
            LOG = HiveMetaStore.LOG;
            alwaysThreadsInitialized = new AtomicBoolean(false);
            threadLocalMS = new ThreadLocal<RawStore>() {
                protected RawStore initialValue() {
                    return null;
                }
            };
            threadLocalTxn = new ThreadLocal<TxnStore>() {
                protected TxnStore initialValue() {
                    return null;
                }
            };
            timerContexts = new ThreadLocal<Map<String, Context>>() {
                protected Map<String, Context> initialValue() {
                    return new HashMap();
                }
            };
            threadLocalConf = new ThreadLocal<Configuration>() {
                protected Configuration initialValue() {
                    return null;
                }
            };
            threadLocalHMSHandler = new ThreadLocal();
            threadLocalModifiedConfig = new ThreadLocal();
            auditLog = LoggerFactory.getLogger(HiveMetaStore.class.getName() + ".audit");
            nextSerialNum = 0;
            threadLocalId = new ThreadLocal<Integer>() {
                protected Integer initialValue() {
                    return HiveMetaStore.HMSHandler.nextSerialNum++;
                }
            };
            threadLocalIpAddress = new ThreadLocal<String>() {
                protected String initialValue() {
                    return null;
                }
            };
            EMPTY_MAP_FM1 = new HashMap(1);
            EMPTY_MAP_FM2 = new HashMap(1);
        }

        private static class StorageDescriptorKey {
            private final StorageDescriptor sd;

            StorageDescriptorKey(StorageDescriptor sd) {
                this.sd = sd;
            }

            StorageDescriptor getSd() {
                return this.sd;
            }

            private String hashCodeKey() {
                return this.sd.getInputFormat() + "\t" + this.sd.getOutputFormat() + "\t" + this.sd.getSerdeInfo().getSerializationLib() + "\t" + this.sd.getCols();
            }

            public int hashCode() {
                return this.hashCodeKey().hashCode();
            }

            public boolean equals(Object rhs) {
                if (rhs == this) {
                    return true;
                } else {
                    return !(rhs instanceof HiveMetaStore.HMSHandler.StorageDescriptorKey) ? false : this.hashCodeKey().equals(((HiveMetaStore.HMSHandler.StorageDescriptorKey)rhs).hashCodeKey());
                }
            }
        }

        private static class PathAndPartValSize {
            public Path path;
            int partValSize;

            PathAndPartValSize(Path path, int partValSize) {
                this.path = path;
                this.partValSize = partValSize;
            }
        }

        private static class PartValEqWrapperLite {
            List<String> values;
            String location;

            PartValEqWrapperLite(Partition partition) {
                this.values = partition.isSetValues() ? partition.getValues() : null;
                this.location = partition.getSd().getLocation();
            }

            public int hashCode() {
                return this.values == null ? 0 : this.values.hashCode();
            }

            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                } else if (obj != null && obj instanceof HiveMetaStore.HMSHandler.PartValEqWrapperLite) {
                    List<String> lhsValues = this.values;
                    List<String> rhsValues = ((HiveMetaStore.HMSHandler.PartValEqWrapperLite)obj).values;
                    if (lhsValues != null && rhsValues != null) {
                        if (lhsValues.size() != rhsValues.size()) {
                            return false;
                        } else {
                            for(int i = 0; i < lhsValues.size(); ++i) {
                                String lhsValue = (String)lhsValues.get(i);
                                String rhsValue = (String)rhsValues.get(i);
                                if (lhsValue == null && rhsValue != null || lhsValue != null && !lhsValue.equals(rhsValue)) {
                                    return false;
                                }
                            }

                            return true;
                        }
                    } else {
                        return lhsValues == rhsValues;
                    }
                } else {
                    return false;
                }
            }
        }

        private static class PartValEqWrapper {
            Partition partition;

            PartValEqWrapper(Partition partition) {
                this.partition = partition;
            }

            public int hashCode() {
                return this.partition.isSetValues() ? this.partition.getValues().hashCode() : 0;
            }

            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                } else if (obj != null && obj instanceof HiveMetaStore.HMSHandler.PartValEqWrapper) {
                    Partition p1 = this.partition;
                    Partition p2 = ((HiveMetaStore.HMSHandler.PartValEqWrapper)obj).partition;
                    if (p1.isSetValues() && p2.isSetValues()) {
                        if (p1.getValues().size() != p2.getValues().size()) {
                            return false;
                        } else {
                            for(int i = 0; i < p1.getValues().size(); ++i) {
                                String v1 = (String)p1.getValues().get(i);
                                String v2 = (String)p2.getValues().get(i);
                                if ((v1 != null || v2 != null) && (v1 == null || !v1.equals(v2))) {
                                    return false;
                                }
                            }

                            return true;
                        }
                    } else {
                        return p1.isSetValues() == p2.isSetValues();
                    }
                } else {
                    return false;
                }
            }
        }
    }

    private static final class ChainedTTransportFactory extends TTransportFactory {
        private final TTransportFactory parentTransFactory;
        private final TTransportFactory childTransFactory;

        private ChainedTTransportFactory(TTransportFactory parentTransFactory, TTransportFactory childTransFactory) {
            this.parentTransFactory = parentTransFactory;
            this.childTransFactory = childTransFactory;
        }

        public TTransport getTransport(TTransport trans) {
            return this.childTransFactory.getTransport(this.parentTransFactory.getTransport(trans));
        }
    }
}

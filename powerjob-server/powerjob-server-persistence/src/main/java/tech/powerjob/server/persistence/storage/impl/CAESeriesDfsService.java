package tech.powerjob.server.persistence.storage.impl;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.env.Environment;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.common.utils.CommonUtils;
import tech.powerjob.common.utils.NetUtils;
import tech.powerjob.server.common.constants.SwitchableStatus;
import tech.powerjob.server.common.spring.condition.PropertyAndOneBeanCondition;
import tech.powerjob.server.extension.dfs.DFsService;
import tech.powerjob.server.extension.dfs.DownloadRequest;
import tech.powerjob.server.extension.dfs.FileLocation;
import tech.powerjob.server.extension.dfs.FileMeta;
import tech.powerjob.server.extension.dfs.StoreRequest;
import tech.powerjob.server.persistence.storage.AbstractDFsService;

import javax.annotation.Priority;
import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * CAE 特性类似的数据库存储
 * PS1. 通过MySqlSeriesDfsService.java改造而来
 * ********************* 配置项 *********************
 * oms.storage.dfs.cae_series.driver
 * oms.storage.dfs.cae_series.url
 * oms.storage.dfs.cae_series.username
 * oms.storage.dfs.cae_series.password
 * oms.storage.dfs.cae_series.auto_create_table
 * oms.storage.dfs.cae_series.table_name
 */
@Slf4j
@Priority(value = Integer.MAX_VALUE - 2)
@Conditional(CAESeriesDfsService.CAESeriesCondition.class)
public class CAESeriesDfsService extends AbstractDFsService {

    private DataSource dataSource;

    private static final String TYPE_CAE = "cae_series";

    /**
     * 数据库驱动，CAEDB SQL Server 为 com.cae.cloudjdbc.Driver
     */
    private static final String KEY_DRIVER_NAME = "driver";
    /**
     * 数据库地址，比如 jdbc:cae://localhost:5138/powerjob-daily
     */
    private static final String KEY_URL = "url";
    /**
     * 数据库账号，比如 SYSDBA
     */
    private static final String KEY_USERNAME = "username";
    /**
     * 数据库密码
     */
    private static final String KEY_PASSWORD = "password";
    /**
     * 是否自动建表
     */
    private static final String KEY_AUTO_CREATE_TABLE = "auto_create_table";
    /**
     * 表名
     */
    private static final String KEY_TABLE_NAME = "table_name";

    /* ********************* SQL region ********************* */

    private static final String DEFAULT_TABLE_NAME = "powerjob_files";

    private static final String CREATE_TABLE_SQL = "CREATE TABLE\n" +
            "IF\n" +
            "\tNOT EXISTS %s (\n" +
            "\t\t`id` BIGINT IDENTITY(1,1) NOT NULL COMMENT 'ID',\n" +
            "\t\t`bucket` VARCHAR ( 255 ) NOT NULL COMMENT '分桶',\n" +
            "\t\t`name` VARCHAR ( 255 ) NOT NULL COMMENT '文件名称',\n" +
            "\t\t`version` VARCHAR ( 255 ) NOT NULL COMMENT '版本',\n" +
            "\t\t`meta` VARCHAR ( 255 ) COMMENT '元数据',\n" +
            "\t\t`length` BIGINT NOT NULL COMMENT '长度',\n" +
            "\t\t`status` INT NOT NULL COMMENT '状态',\n" +
            "\t\t`data` BLOB NOT NULL COMMENT '文件内容',\n" +
            "\t\t`extra` VARCHAR ( 255 ) COMMENT '其他信息',\n" +
            "\t\t`gmt_create` DATETIME NOT NULL COMMENT '创建时间',\n" +
            "\t\t`gmt_modified` DATETIME COMMENT '更新时间',\n" +
            "\tPRIMARY KEY ( `id` ) \n" +
            "\t);";

    private static final String INSERT_SQL = "insert into %s(`bucket`, `name`, `version`, `meta`, `length`, `status`, `data`, `extra`, `gmt_create`, `gmt_modified`) values (?,?,?,?,?,?,?,?,?,?);";

    private static final String DELETE_SQL = "DELETE FROM %s ";

    private static final String QUERY_FULL_SQL = "select * from %s";

    private static final String QUERY_META_SQL = "select `bucket`, `name`, `version`, `meta`, `length`, `status`, `extra`, `gmt_create`, `gmt_modified` from %s";


    private void deleteByLocation(FileLocation fileLocation) {
        String dSQLPrefix = fullSQL(DELETE_SQL);
        String dSQL = dSQLPrefix.concat(whereSQL(fileLocation));
        executeDelete(dSQL);
    }

    private void executeDelete(String sql) {
        try (Connection con = dataSource.getConnection()) {
            con.createStatement().executeUpdate(sql);
        } catch (Exception e) {
            log.error("[CAESeriesDfsService] executeDelete failed, sql: {}", sql);
        }
    }

    @Override
    public void store(StoreRequest storeRequest) throws IOException {

        Stopwatch sw = Stopwatch.createStarted();
        String insertSQL = fullSQL(INSERT_SQL);

        FileLocation fileLocation = storeRequest.getFileLocation();

        // 覆盖写，写之前先删除
        deleteByLocation(fileLocation);

        Map<String, Object> meta = Maps.newHashMap();
        meta.put("_server_", NetUtils.getLocalHost());
        meta.put("_local_file_path_", storeRequest.getLocalFile().getAbsolutePath());

        Date date = new Date(System.currentTimeMillis());

        try (Connection con = dataSource.getConnection()) {
            PreparedStatement pst = con.prepareStatement(insertSQL);

            pst.setString(1, fileLocation.getBucket());
            pst.setString(2, fileLocation.getName());
            pst.setString(3, "mu");
            pst.setString(4, JsonUtils.toJSONString(meta));
            pst.setLong(5, storeRequest.getLocalFile().length());
            pst.setInt(6, SwitchableStatus.ENABLE.getV());
            pst.setBlob(7, new BufferedInputStream(Files.newInputStream(storeRequest.getLocalFile().toPath())));
            pst.setString(8, null);
            pst.setDate(9, date);
            pst.setDate(10, date);

            pst.execute();

            log.info("[CAESeriesDfsService] store [{}] successfully, cost: {}", fileLocation, sw);

        } catch (Exception e) {
            log.error("[CAESeriesDfsService] store [{}] failed!", fileLocation);
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void download(DownloadRequest downloadRequest) throws IOException {

        Stopwatch sw = Stopwatch.createStarted();
        String querySQL = fullSQL(QUERY_FULL_SQL);

        FileLocation fileLocation = downloadRequest.getFileLocation();

        FileUtils.forceMkdirParent(downloadRequest.getTarget());

        try (Connection con = dataSource.getConnection()) {

            ResultSet resultSet = con.createStatement().executeQuery(querySQL.concat(whereSQL(fileLocation)));

            boolean exist = resultSet.next();

            if (!exist) {
                log.warn("[CAESeriesDfsService] download file[{}] failed due to not exits!", fileLocation);
                return;
            }

            Blob dataBlob = resultSet.getBlob("data");
            FileUtils.copyInputStreamToFile(new BufferedInputStream(dataBlob.getBinaryStream()), downloadRequest.getTarget());

            log.info("[CAESeriesDfsService] download [{}] successfully, cost: {}", fileLocation, sw);

        } catch (Exception e) {
            log.error("[CAESeriesDfsService] download file [{}] failed!", fileLocation, e);
            ExceptionUtils.rethrow(e);
        }

    }

    @Override
    public Optional<FileMeta> fetchFileMeta(FileLocation fileLocation) throws IOException {

        String querySQL = fullSQL(QUERY_META_SQL);

        try (Connection con = dataSource.getConnection()) {

            ResultSet resultSet = con.createStatement().executeQuery(querySQL.concat(whereSQL(fileLocation)));

            boolean exist = resultSet.next();

            if (!exist) {
                return Optional.empty();
            }

            FileMeta fileMeta = new FileMeta()
                    .setLength(resultSet.getLong("length"))
                    .setLastModifiedTime(resultSet.getDate("gmt_modified"))
                    .setMetaInfo(JsonUtils.parseMap(resultSet.getString("meta")));
            return Optional.of(fileMeta);

        } catch (Exception e) {
            log.error("[CAESeriesDfsService] fetchFileMeta [{}] failed!", fileLocation);
            ExceptionUtils.rethrow(e);
        }

        return Optional.empty();
    }

    @Override
    public void cleanExpiredFiles(String bucket, int days) {

        // 虽然官方提供了服务端删除的能力，依然强烈建议用户直接在数据库层面配置清理事件！！！

        String dSQLPrefix = fullSQL(DELETE_SQL);
        final long targetTs = DateUtils.addDays(new Date(System.currentTimeMillis()), -days).getTime();
        final String targetDeleteTime = CommonUtils.formatTime(targetTs);
        log.info("[CAESeriesDfsService] start to cleanExpiredFiles, targetDeleteTime: {}", targetDeleteTime);
        String fSQL = dSQLPrefix.concat(String.format(" where gmt_modified < '%s'", targetDeleteTime));
        log.info("[CAESeriesDfsService] cleanExpiredFiles SQL: {}", fSQL);
        executeDelete(fSQL);
    }

    @Override
    protected void init(ApplicationContext applicationContext) {

        Environment env = applicationContext.getEnvironment();

        CAEProperty caeProperty = new CAEProperty()
                .setDriver(fetchProperty(env, TYPE_CAE, KEY_DRIVER_NAME))
                .setUrl(fetchProperty(env, TYPE_CAE, KEY_URL))
                .setUsername(fetchProperty(env, TYPE_CAE, KEY_USERNAME))
                .setPassword(fetchProperty(env, TYPE_CAE, KEY_PASSWORD))
                .setAutoCreateTable(Boolean.TRUE.toString().equalsIgnoreCase(fetchProperty(env, TYPE_CAE, KEY_AUTO_CREATE_TABLE)));

        try {
            initDatabase(caeProperty);
            initTable(caeProperty);
        } catch (Exception e) {
            log.error("[CAESeriesDfsService] init datasource failed!", e);
            ExceptionUtils.rethrow(e);
        }

        log.info("[CAESeriesDfsService] initialize successfully, THIS_WILL_BE_THE_STORAGE_LAYER.");
    }

    void initDatabase(CAEProperty property) {

        log.info("[CAESeriesDfsService] init datasource by config: {}", property);

        HikariConfig config = new HikariConfig();

        config.setDriverClassName(property.driver);
        config.setJdbcUrl(property.url);
        config.setUsername(property.username);
        config.setPassword(property.password);

        config.setAutoCommit(true);
        // 池中最小空闲连接数量
        config.setMinimumIdle(2);
        // 池中最大连接数量
        config.setMaximumPoolSize(32);

        dataSource = new HikariDataSource(config);
    }

    void initTable(CAEProperty property) throws Exception {

        if (property.autoCreateTable) {

            String createTableSQL = fullSQL(CREATE_TABLE_SQL);

            log.info("[CAESeriesDfsService] use create table SQL: {}", createTableSQL);
            try (Connection connection = dataSource.getConnection()) {
                connection.createStatement().execute(createTableSQL);
                log.info("[CAESeriesDfsService] auto create table successfully!");
            }
        }
    }

    private String fullSQL(String sql) {
        return String.format(sql, parseTableName());
    }

    private String parseTableName() {
        // 误删，兼容本地 unit test
        if (applicationContext == null) {
            return DEFAULT_TABLE_NAME;
        }
        String tableName = fetchProperty(applicationContext.getEnvironment(), TYPE_CAE, KEY_TABLE_NAME);
        return StringUtils.isEmpty(tableName) ? DEFAULT_TABLE_NAME : tableName;
    }

    private static String whereSQL(FileLocation fileLocation) {
        return String.format(" where `bucket`='%s' AND `name`='%s' ", fileLocation.getBucket(), fileLocation.getName());
    }

    @Override
    public void destroy() throws Exception {
    }

    @Data
    @Accessors(chain = true)
    static class CAEProperty {
        private String driver;
        private String url;
        private String username;
        private String password;

        private boolean autoCreateTable;
    }

    public static class CAESeriesCondition extends PropertyAndOneBeanCondition {
        @Override
        protected List<String> anyConfigKey() {
            return Lists.newArrayList("oms.storage.dfs.cae_series.url");
        }

        @Override
        protected Class<?> beanType() {
            return DFsService.class;
        }
    }
}

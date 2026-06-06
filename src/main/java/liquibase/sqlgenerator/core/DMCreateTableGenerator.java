package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.DMDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.CreateTableStatement;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DM 数据库 CREATE TABLE SQL 生成器.
 *
 * Bug 修复:
 * 1. Liquibase 4.x core 生成的 CREATE TABLE 不带 IF NOT EXISTS.
 * 2. DM 的 DDL 会隐式提交, autoCommit=false 时 DDL 立即对其他连接可见,
 *    但同一事务内的 DML (INSERT INTO DATABASECHANGELOG) 还未提交.
 *    多模块并发启动 (K8s, 同一 schema, 独立 changelog 表) 时,
 *    模块 A 的 CREATE TABLE 对模块 B 可见, 但 A 的 changelog 记录对 B 不可见,
 *    B 认为该 changeset 未执行, 再次执行 CREATE TABLE → 对象已存在.
 *
 * 修法: 所有 CREATE TABLE 统一加 IF NOT EXISTS, 使其幂等.
 */
public class DMCreateTableGenerator extends CreateTableGenerator {

    /** 匹配 CREATE TABLE 后跟表名 (兼容引号包裹的 schema 前缀) */
    private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(
            "(?i)(CREATE\\s+TABLE)(\\s+(?:\".*?\"\\.)?(?:\".*?\"|[A-Za-z_][A-Za-z0-9_$]*))"
    );

    @Override
    public int getPriority() {
        // 跟默认 CreateTableGenerator 同优先级, 但因为 supports() 限定 DMDatabase 所以不会冲突
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateTableStatement statement, Database database) {
        // 只接管 DMDatabase 的 CreateTable, 其他数据库走默认生成器
        return database instanceof DMDatabase;
    }

    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        // 先按默认逻辑生成 SQL
        Sql[] generated = sqlGeneratorChain.generateSql(statement, database);

        // 没有生成或非 DMDatabase, 不做处理
        if (generated == null || generated.length == 0) {
            return generated;
        }

        // 表名: 用 statement 直接拿 (而非 escapeTableName, 避免大小写转换)
        String tableName = statement.getTableName();
        if (tableName == null) {
            return generated;
        }
        // 改写每个 Sql: 把 "CREATE TABLE" 改成 "CREATE TABLE IF NOT EXISTS"
        for (int i = 0; i < generated.length; i++) {
            Sql sql = generated[i];
            if (sql == null) continue;
            String original = sql.toSql();
            if (original == null) continue;

            // 已包含 IF NOT EXISTS 不重复加
            if (original.toUpperCase().contains("IF NOT EXISTS")) {
                continue;
            }
            String rewritten = rewriteCreateTable(original);
            if (rewritten != null && !rewritten.equals(original)) {
                generated[i] = new UnparsedSql(rewritten);
            }
        }
        return generated;
    }

    /**
     * 把 SQL 里所有 "CREATE TABLE" 后追加 " IF NOT EXISTS" (不重复加).
     */
    private String rewriteCreateTable(String sql) {
        Matcher m = CREATE_TABLE_PATTERN.matcher(sql);
        StringBuffer sb = new StringBuffer();
        boolean changed = false;
        while (m.find()) {
            String head = m.group(1);  // "CREATE TABLE"
            String tablePart = m.group(2);  // " [schema.]tablename"
            String replacement = head + " IF NOT EXISTS" + tablePart;
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            changed = true;
        }
        m.appendTail(sb);
        return changed ? sb.toString() : sql;
    }
}

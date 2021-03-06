<?php
namespace Topnew;

/**
 * Topnew DB v 2019.10.10 - PDO database library
 * The MIT License (MIT) Copyright (c) 1998-2018, Topnew Geo, topnew.net
 *
 * conn ... [host,user,pass,...] connect to database [,db,eng,char,port,enc,debug,log,slow]
 * debug .. show ............... toggle SQL debug
 * err .... msg ................ SQL err code or msg
 * run .... sql para ........... run any SQL
 *
 * $conn = ['host' => 'localhost', 'user' => 'username', 'pass' => 'password'];
 * $db = new Topnew\Db($conn);
 *
 * SELECT : val() row() rows() list() lists() enum() all() arr() arr2() arr4All() arrAll() arrAll3()
 *
 * $email = $db->select('email')->from('user')->where('id', 123)->val();
 * SELECT email FROM user WHERE id = 123
 *
 * $ids = $db->select('id')->from('user')->where('sex', 'M')->enum();
 * SELECT id FROM user WHERE sex = 'M'
 *
 * $list = $db->select('id, name')->from('user')->where('id', [1,2,3])->list();
 * SELECT id, name FROM user WHERE id IN (1,2,3)
 *
 * $row = $db->from('user')->where('id', 1)->row();
 * SELECT * FROM user WHERE id = 1
 * // [id => 1, name => Topnew, sex = M, ...]
 *
 * $row = $db->from('user')->where('id', 1)->row('num');
 * // [0 => 1, 1 => Topnew, 3 => M, ...]
 * $row = $db->from('user')->where('id', 1)->row('both');
 * // [0 => 1, id => 1, 1 => Topnew, name => Topnew, 3 => M, ...]
 *
 * $rows = $db->select('distinct', 'name')->from('user')->where('id', '!=', [2,3,4])->rows();
 * SELECT DISTINCT name FROM user WHERE id NOT IN (2,3,4)
 *
 * $rows = $db->from('user')->rows('id'); ## row('id') == rows('id') == all('id')
 * // result in assoc arrow of id => row
 *
 * $rows = $db->rows('SELECT * FROM user WHERE id > :id', ['id' => 3]);
 *
 * $num = $db->table('user')->where(...)->count()
 *
 * arrAllX  sql para level all . select X multi-dimensional array -- arr | arr3 | arrAll3 | arr3All
 * enum ... sql para ........... select array(val, val, ...)
 * list(s). sql para ........... select array(key => val, ...)
 * row .... sql para mode ...... select a row(s) ASSOC | NUM | BOTH | KEY (pls use arr instead)
 * rows|all sql para mode ...... select all rows ASSOC | NUM | BOTH | KEY (pls use arr instead)
 * val .... sql para ........... select a value
 * count .. sql (default *) .... SELECT count(*) -- this method only works in chained
 *
 * row(mode) rows(mode) all(mode) when mode = ASSOC | NUM | BOTH
 *
 * arr() == enum() when SQL select 1 col only
 * arr() == list() when SQL select 2 cols
 * arr() == rows() when SQL first col is key(mode)
 *
 * $data = ['name' => 'topnew', 'created' => 'now()'];
 * $last_id = $db->table('user')->data($data)->insert();
 * $last_id = $db->data($data)->insert('user');
 * $last_id = $db->insert('user', $data);
 * INSERT INTO user(name, created) VALUES('Topnew', now())
 *
 * $db->data($data)->replace('user');
 *
 * $db->table('user')->where('id', 123)->delete();
 * $db->where('id', 123)->delete('user');
 * $db->delete('user', ['id' => 123]); ## not recommended as it is easily mis-typed as ['id', 123]
 *
 * $db->table('user')->data($data)->where('id', 123)->update(); ## method order not mind as long as update() is last one
 * $db->data($data)->where('id', 123)->update('user');
 * $db->where('id', 123)->update('user', $data);
 * $db->update('user', $data, ['id' => 123]); ## not recommended as it is easily mis-typed as ['id', 123]
 *
 * $id = $db->data($data)->where(...)->save('user') ## update if where found else insert
 *
 * insert . tab data pgLast .... eg data = [key=>val, ...] return lastInsertId
 * replace. tab data ........... eg data = [key=>val, ...]
 * delete . tab .... where para. eg where= 'where col=val ...' OR where = [...]
 * delete . tab wher para ......
 * update . tab data where para.
 * save ... tab data where para. insert | update if where found
 *
 * above where-para: string where + para[key=>val] OR array[where] no para
 * eg where = ['col1=col2', 'col3'=>123, ...]
 * eg where = ['where' => ['col1=col2', 'col3'=>123, ...], 'order'=>..., 'limit'=>...]
 * for more example of $where please refer docs at selectWhere()
 *
 * chained methods (join, having, where can be used for multiple times) : $this
 * ->select() : SQL | (distinct,) col, [alias=>col2], ...| [(distinct,) col, alias=>col2, ...]
 * ->from  () : SQL | tab, [alias=>tab2], [alias=>SQL],..| [tab, alias=>tab2, tab3, ...]
 * ->join  () : SQL | (alias=>)tab, on-where, left/right | [[tab, on, L/R], join2, ....]
 * ->where () : SQL | col, op, val, is_col
 * ->where () : [SQL, col=>val, [col,val], [col,op,val,is_col], [NOT,SQL], [OR,where], [(not) exist(s), SQL], ...]
 * ->group () : SQL | col, 2, col3, ............| [col, 2, col3, ...]
 * ->order () : SQL | col, 2, [col3=>desc], ....| [col, 2, cols=>desc, ...]
 * ->limit () : SQL | X | [X] | X, Y | [X, Y] ..| [X, page=>Y]
 * ->having() : see where()
 * ->table () : alias of from()
 * ->page  () : Y ....................... (only for chained method)
 * ->leftJoin () : (alias=>)tab, on-where (only for chained method)
 * ->rightJoin() : (alias=>)tab, on-where (only for chained method)
 * ->val() | ->row() | -> all() | ->rows() | ->arr() | ->enum() | ->list()
 *
 * You can also pass $sql directly eg $row = $db->row($sql, $param) etc, here $sql can be SQL or
 * $sql = [select=>, from=>, join=>, where=>, group=>, having=>, order=>, limit=>] see above
 */

class Db
{
    private $conn = [
        'host' => 'localhost',
        'user' => 'topnew',
        'pass' => 'geo',
        'db'   => 'topnew',
        'eng'  => 'mysql',
        'char' => 'utf8',
        'port' => 3306,
        'enc'  => 0,    // if 1 need Topnew\Auth::dec()
        'debug'=> null, // all (SQL) | err (only) | (no show)
        'log'  => '',   // log table for SQL err or slow SQL eg log_err
        'slow' => 1000, // SQL slower than this will be logged
    ];
    private $pdo  = null;
    private $sql  = []; // SQL or [] check sql()
    private $bind = [];
    private $last_insert_id = 0;
    public  $ttl = 0; // count of SELECT before limit x,y

    public static function make($conn = []) { return new self($conn); }

    public function __construct($conn = [], $sql = []) {
        $this->sql = $sql;
        return $this->conn($conn);
    }

    public function conn($conn = []) {
        if (is_array($conn)) {
            foreach ($conn as $k => $v) {
                if (array_key_exists($k, $this->conn) && !is_array($v)) {
                    $this->conn[$k] = $v;
                }
            }
        }
        if (!in_array($this->conn['eng'], ['pgsql', 'sqlite', 'dblib'])) {
            $this->conn['eng'] = 'mysql'; // dblib == mssql
        }

        $conn = $this->conn;
        $this->debug($conn['debug']);
        if ($conn['eng'] == 'sqlite') {
            $this->pdo = new \PDO('sqlite:' . $conn['db']);
            return $this->pdo;
        }
        if ($conn['enc']) {
            $conn['pass'] = Auth::dec($conn['pass']);
        }

        $options = ('mysql' == $conn['eng'] && 'utf8' == $conn['char']) ? [\PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8'] : [];
        $pdo = $conn['eng'] . ':host=' . $conn['host'] . ';dbname=' . $conn['db'];
        if ($conn['port'] && (
                ($conn['eng'] == 'mysql' && $conn['port'] != 3306) ||
                ($conn['eng'] == 'pgsql' && $conn['port'] != 5432)
            )) {
            $pdo .= ';port='. $conn['port'];
        }

        try {
            $pdo = new \PDO($pdo, $conn['user'], $conn['pass'], $options);
            $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);
            $this->pdo = $pdo;
            return $pdo;
        } catch (\PDOException $e) {
            echo 'Connection Failed: ' . $e->getMessage();
        }
    }

    // PART 1 - GET SQL results eg get val, row, enum, list, arr, all, rows etc

    public function __call($name, $args) {
        $sql = isset($args[0]) ? $args[0] : $this->sql;
        $param = isset($args[1]) ? $args[1] : '';
        if ($name == 'list') { // alias of lists()
            return $this->lists($sql, $param);
        } elseif (substr($name, 0, 3) == 'arr') { // alias of arr(All)X() == arr(,,X, All)
            $all = 0;
            if (substr($name, 3, 3) == 'All') { // arrAllX()
                $all = 1;
                $name = substr($name, 3);
            } elseif (substr($name, -3) == 'All') { // or arrXAll()
                $all = 1;
            }
            $level = ceil(substr($name, 3));
            return $this->arr($sql, $param, $level, $all);
        }
        echo "\n", '<br><span style="color:red">Err: calling method not exists: DB::' . $name . '()</span>';
    }

    public function all($sql = '', $param = '', $mode = '') {
        return $this->rows($sql, $param, $mode); // alias of rows
    }

    public function arr($sql = '', $param = '', $level = 0, $all_record = 0) {
        /**
         * get multiple level of arrays of data from sql eg row = [a, b, c, d]
         * level=0: same as level=1 * see more notes below
         * level=1: [a] = [b, c, d] == rows(,,'a') when first col is key
         * level=2: [a][b] = [c, d]
         * level=3: [a][b][c] = d
         * level=4+:same as level=3
         *
         * all_record=0: same array keys found, last value will be final
         * all_record=1: result will be in array for same array keys
         *
         * level=0 and cols=1 == enum()
         * level=0 and cols=2 == list()
         *
         * alias eg arr(All)X(All)() will triggered by __call() of arr($sql, $param, X, all)
         */
        $rows = $this->rows($sql, $param);
        if (!count($rows) || !is_array($rows[0])) {
            return [];
        }
        $num = count($rows[0]);
        if (!$num) {
            return [];
        }

        $level = ($level >= $num) ? $num - 1 : $level;
        $data = [];
        if ($level < 1) {
            if ($num == 1) {
                foreach ($rows as $r) {
                    $data[] = reset($r);
                }
                return $data; // enum
            } else {
                $level = 1; // if num == 2 same as list()
            }
        }

        $keys = array_slice(array_keys($rows[0]), 0, $level);
        foreach ($rows as $r) {
            $ref = &$data[$r[$keys[0]]];
            foreach ($keys as $j => $k) {
                if ($j) {
                    $ref = &$ref[$r[$k]];
                }
                unset($r[$k]);
            }
            $r = count($r) > 1 ? $r : reset($r);
            if ($all_record) {
                $ref[] = $r;
            } else {
                $ref = $r;
            }
        }
        return $data;
    }

    public function count($sql = '*') {
        $this->sql['field'] = 'COUNT(' . (strlen($sql) ? $sql : '*') . ')';
        return $this->val();
    }

    public function enum($sql = '', $param = '') {
        // get enum array of one val == arr($sql) when there is only 1 col in select
        $rows = $this->row($sql, $param, 'ALL NUM');
        $enum = [];
        foreach ($rows as $r) {
            $enum[] = isset($r[0]) ? $r[0] : null;
        }
        return $enum;
    }

    public function lists($sql = '', $param = '') {
        // get list array of key=>val == arr($sql) when there is exactly 2 cols in select
        // also alias of list() triggered by __call()
        $rows = $this->row($sql, $param, 'ALL NUM');
        $list = [];
        foreach ($rows as $r) {
            if (isset($r[0])) {
                $list[$r[0]] = isset($r[1]) ? $r[1] : null;
            }
        }
        return $list;
    }

    public function pgno($pgno = 1, &$ttl = 0, &$max = 0) {
        $this->ttl();
        $pgno = max($pgno, 1);
        $this->page($pgno);
        if ($this->sql['limit'][0]) {
            $pgsize = $this->sql['limit'][0];
        } else {
            $pgsize = $this->sql['limit'][0] = 15; // without pgsize, make no sense to call this. defa to 15
        }
        $stmt = $this->run();
        $ttl = $this->ttl;
        $max = ceil($ttl / $pgsize); // ttl pages
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public function row($sql = '', $param = '', $mode = '') {
        if ($sql && !is_array($sql) && !$param && !$mode) {
            if (in_array(strtoupper($sql), ['ASSOC', 'NUM', 'BOTH', 'ALL ASSOC', 'ALL NUM', 'ALL BOTH'])) {
                $mode = $sql;
                $sql = '';
            }
        }

        $stmt = $this->run($sql, $param);
        if (!$stmt) {
            return [];
        }
        $MODE = $mode ? strtoupper($mode) : 'ASSOC';
        if ($MODE == 'ASSOC') {
            return $stmt->fetch(\PDO::FETCH_ASSOC);
        } elseif ($MODE == 'NUM') {
            return $stmt->fetch(\PDO::FETCH_NUM);
        } elseif ($MODE == 'BOTH') {
            return $stmt->fetch();
        } elseif ($MODE == 'ALL NUM') {
            return $stmt->fetchAll(\PDO::FETCH_NUM);
        } elseif ($MODE == 'ALL BOTH') {
            return $stmt->fetchAll();
        }
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        if ($MODE == 'ALL ASSOC') {
            return $rows;
        } elseif (!count($rows) || !in_array($mode, array_keys($rows[0]))) {
            return [];
        }
        $arr = [];
        foreach ($rows as $r) {
            $arr[$r[$mode]] = $r; // pk
        }
        return $arr;
    }

    public function rows($sql = '', $param = '', $mode = '') { // get all rows
        if ($sql && !is_array($sql) && !$param && !$mode && in_array(strtoupper($sql), ['ASSOC', 'NUM', 'BOTH'])) {
            $mode = $sql;
            $sql = '';
        }
        $MODE = $mode ? strtoupper($mode) : 'ASSOC';
        $mode = in_array($MODE, ['NUM', 'BOTH', 'ASSOC']) ? 'ALL ' . $MODE : $mode;
        return $this->row($sql, $param, $mode);
    }

    public function val($sql = '', $param = '') { // get one val
        $row = $this->row($sql, $param);
        return is_array($row) ? array_shift($row) : $row;
    }

    // PART 2 - General db functions eg insert delete update connect ...

    public function debug($show = 'all') { // show = all (SQL) | err (only)| -1 (no show)
        $this->conn['debug'] = $show == -1 ? null : $show;
    }

    public function delete($tab = '', $data = '', $where = '', $param = '') {
        // if $data is not null, $data is $where, $where is $para -- i.e. delete($tab, $where, $para)
        if ($data) {
            $param = $where;
            $where = $data;
        }
        if ($tab) {
            $this->from($tab);
        }
        $sql = 'DELETE '
            . $this->selectFrom($this->sql)
            . $this->selectJoin($this->sql)
            . $this->bindWhere($where ?: (isset($this->sql['where']) ? $this->sql['where'] : ''), $param)
            . $this->selectOrder($this->sql)
            . $this->selectLimit($this->sql);
        $this->sql = []; // need reset
        return $this->run($sql);
    }

    public function err($msg = 0) {
        // Return SQL Error Code or Error Msg when msg = 1
        $err = $this->pdo()->errorCode();
        if ($err && '00000' != $err) {
            $err = $this->pdo()->errorInfo();
            return str_replace('<', '&lt;', $err[1] . ($msg ? ': '. $err[2] : ''));
        }
    }

    public function insert($tab = '', $data = '', $cmd = '') {
        // $cmd = INSERT | REPLACE | $pg_insert_id
        $pg_insert_id = $cmd;
        $cmd  = (strtoupper($cmd) == 'REPLACE') ? 'REPLACE' : 'INSERT';
        $pg_insert_id = ($pg_insert_id && $cmd == 'INSERT' && $this->conn['eng'] == 'pgsql') ? ' RETURNING ' . $pg_insert_id : '';
        $data = $data ?: (isset($this->sql['data']) ? $this->sql['data'] : null);
        $data = $this->bindData($data);
        $tab  = $this->selectKeyw($tab) ?: $this->selectFrom($this->sql, 0);
        $cols = array_keys($data);
        foreach ($cols as $i => $c) {
            $cols[$i] = $this->selectKeyw($c);
        }
        $stmt = $this->run($cmd .' INTO ' . $tab . '(' . implode(', ', $cols) . ")\nVALUES (" . implode(', ', $data) . ')' . $pg_insert_id);
        $this->sql = []; // reset
        if ($pg_insert_id) {
            $row = $stmt->fetch(\PDO::FETCH_NUM);
            $this->last_insert_id = $row[0];
        }
        return $this->last_insert_id;
    }

    public function replace($tab = '', $data = '') {
        return $this->insert($tab, $data, 'REPLACE');
    }

    public function run($sql = '', $param = '') {
        $sql = $sql ?: $this->sql;
        $this->bind($param);
        if (!$sql) {
            return;
        } elseif (is_array($sql)) {
            $sql = $this->sql($sql, '', $sql_ttl);
        }
        $time_start = microtime(true);
        $stmt = $this->pdo()->prepare($sql);
        $bind = is_array($this->bind) && $this->bind ? $this->bind : null;
        if ($stmt) {
            $stmt->execute($bind);
        }
        $err = $this->err(1); // must immediately after execute
        $this->last_insert_id = $this->pdo()->lastInsertId(); // must before log() to avoid INSERT
        $this->log($sql, $time_start, $err);

        if (isset($sql_ttl) && $sql_ttl) {
            $stmt2 = $this->pdo()->prepare($sql_ttl);
            if ($stmt2) {
                $stmt2->execute($bind);
                $row = $stmt2->fetch(\PDO::FETCH_NUM);
                $this->ttl = reset($row);
            }
        }

        $this->bind = []; // reset after run
        return $stmt;
    }

    public function save($tab = '', $data = '', $where = '', $param = '') {
        // if $where found update else insert
        $tab = $tab ?: $this->selectFrom($this->sql, 0);
        $where = $this->bindWhere($where ?: (isset($this->sql['where']) ? $this->sql['where'] : ''), $param);
        if ($where && $this->val('SELECT 1 FROM ' . $tab . ' ' . $where . (stripos($where, 'limit ') === false ? ' LIMIT 1' : ''))) {
            $this->update($tab, $data, $where); // no return
        } else {
            return $this->insert($tab, $data);
        }
    }

    public function update($tab = '', $data = '', $where = '', $param = '') {
        $data = $this->bindData($data ?: (isset($this->sql['data']) ? $this->sql['data'] : ''));
        if ($data && is_array($data)) {
            foreach ($data as $k => $v) {
                $data[$k] = is_numeric($k) ? $v : $k . ' = ' . $v;
            }
            $data = implode(', ', $data);
        }

        $sql = 'UPDATE '
            . ($tab ?: $this->selectFrom($this->sql, 0))
            . $this->selectJoin($this->sql)
            . "\nSET " . $data
            . $this->bindWhere($where ?: (isset($this->sql['where']) ? $this->sql['where'] : ''), $param)
            . $this->selectOrder($this->sql)
            . $this->selectLimit($this->sql);
        $this->sql = []; // need reset
        return $this->run($sql);
    }

    // PART 3 - trained methods

    public function select() { $this->setSql('field', func_get_args()); return $this; }
    public function from()   { $this->setSql('from',  func_get_args()); return $this; }
    public function table()  { $this->setSql('from',  func_get_args()); return $this; }
    public function group()  { $this->setSql('group', func_get_args()); return $this; }
    public function order()  { $this->setSql('order', func_get_args()); return $this; }
    public function limit()  { $this->setSql('limit', func_get_args()); return $this; }
    public function ttl()    { $this->sql['ttl'] = 1;                   return $this; }
    public function where()  { $this->setWhere(func_get_args());        return $this; }
    public function having() { $this->setWhere(func_get_args(), 'hav'); return $this; }
    public function data($data = []) { $this->sql['data'] = $data;      return $this; }

    public function join() {
        $args = func_get_args();
        if (isset($args[0])) {
            $this->sql['join'][] = $args;
        }
        return $this;
    }

    public function leftJoin() {
        $args = func_get_args();
        return $this->join((isset($args[0]) ? $args[0] : null), (isset($args[1]) ? $args[1] : null), 'L');
    }

    public function rightJoin() {
        $args = func_get_args();
        return $this->join((isset($args[0]) ? $args[0] : null), (isset($args[1]) ? $args[1] : null), 'R');
    }

    public function page($page = 0) {
        if (!isset($this->sql['limit'])) {
            $this->sql['limit'] = [0];
        } elseif (!is_array($this->sql['limit'])) {
            $this->sql['limit'] = [$this->sql['limit']];
        } elseif (isset($this->sql['limit'][1])) {
            unset($this->sql['limit'][1]);
        }
        if (!isset($this->sql['limit'][0])) {
            $this->sql['limit'][0] = 0;
        }
        $this->sql['limit']['page'] = $page;
        return $this;
    }

    private function setSql($key = '', $args = null) {
        if (count($args) == 1) {
            $args = $args[0];
        }
        $this->sql[$key] = $args;
    }

    private function setWhere ($args = [], $key = '') {
        if (!isset($args[0]) || !$args[0]) {
            return;
        }
        $key = $key == 'hav' ? 'having' : 'where';
        if (!is_array($args[0])) {
            $args[0] = [$args]; // col, op, val, is_col
        }
        foreach ($args[0] as $k => $v) {
            $this->sql[$key][] = is_numeric($k) ? $v : [$k, $v];
        }
    }

    // PART 4 - generate paramed SQL

    public function sql($param = [], $bind = [], &$sql_ttl = '') {
        $param = $param ?: $this->sql;
        $this->bind($bind);
        if (!$param || !is_array($param)) {
            // might need reset sql here as well ?
            return $param;
        }
        if (isset($param['select']) && !isset($param['field'])) {
            $param['field'] = $param['select'];
        }

        $sql = $this->selectFrom( $param)
            . $this->selectJoin( $param)
            . (isset($param['where']) && $param['where'] ? "\nWHERE" . $this->selectWhere($param['where']) : '')
            . $this->selectGroup($param)
            . $this->selectHaving($param);

        if (isset($param['ttl']) && $param['ttl']) {
            $sql_ttl = 'SELECT count(*)' . $sql;
        }

        $sql = 'SELECT ' . $this->selectField($param) . $sql
            . $this->selectOrder($param)
            . $this->selectLimit($param);
        $this->sql = []; // need reset -- bind reset after run()
        return $sql;
    }

    public function bind($param) {
        if ($param && is_array($param)) {
            foreach ($param as $k => $v) {
                // if $k changed, this method is not safe
                // however it can be treated as user error
                // so developer need to make sure not pass in same k
                $this->bindKey($k, $v);
            }
        }
        return $this;
    }

    private function bindData($data = '') {
        // change data[k]=v to data[k]=:k and param[:k]=v
        if (!$data || !is_array($data)) {
            return $data;
        }
        foreach ($data as $k => $v) {
            if (is_null($v)) {
                $v = 'NULL';
            } elseif (strtoupper($v) == 'NOW()') {
                //
            } elseif (is_numeric($v) && substr($v, 0, 1) != '0') {
                // 0123 need quoted, 0.123 also quoted OK though
            } elseif (!is_numeric($k) || ceil($k) != $k) {
                // only bind possible harmful SQL: # -- /* ; \'
                if (strpos($v, ';')  !== false ||
                    strpos($v, '#')  !== false ||
                    strpos($v, '--') !== false ||
                    strpos($v, '/*') !== false ||
                    //strpos($v, '\\"')  !== false ||
                    //strpos($v, '\\\'') !== false ||
                    strpos($v, '\\') !== false
                ) {
                    $v = $this->bindKey($k, $v);
                } else {
                    $v = "'" . str_replace("'", "''", $v) . "'";
                }
            }
            $data[$k] = $v;
        }
        return $data;
    }

    public function bindKey($k, $v) {
        // add k v into bind and return final key
        $k = ($k[0] == ':') ? substr($k, 1) : $k;
        if (isset($this->bind[':' . $k])) {
            $k = is_numeric($k) ? count($this->bind) : $k . '_' . rand();
            if (isset($this->bind[':' . $k])) {
                // return $this->bindKey($k, $v); -- feel like this is dangerous use next line instead -- should be safe
                $k .= '_' . rand();
                if (isset($this->bind[':' . $k])) {
                    $k .= '_' . rand(); // just in case above line still get duplicate keys, do one more:
                }
            }
        }
        $k = ':' . $k;
        $this->bind[$k] = $v;
        return $k;
    }

    private function bindWhere($where = '', $param) {
        $this->bind($param);
        if (!is_array($where)) {
            return $where;
        } elseif (isset($where['where']) && (isset($where['order']) || isset($where['limit']))) {
            $limit = $this->selectOrder($where) . $this->selectLimit($where);
            $where = $where['where'];
        }
        $where = $this->selectWhere($where, ' AND');
        return "\n" . (strlen(trim($where)) ? 'WHERE ' : '') . $where . (isset($limit) ? $limit : '');
    }

    private function log($sql, $time_start, $err) {
        $time_end = microtime(true);
        $time = round(($time_end - $time_start) * 1000);
        $url = '';
        if ($err) {
            $bt = debug_backtrace();
            $err .= "\n" . '-- Debug Backtrace ----------';
            $url = '';
            foreach ($bt as $arr) {
                $url = (!$url && $arr['file'] != __FILE__) ? $arr['file'] : $url;
                if (isset($arr['file'])) {
                    // some bt no file or line
                    $err .= "\nL" . str_pad($arr['line'], 3, 0, STR_PAD_LEFT)
                        . ': ' . $arr['function'] . '() ' . $arr['file'];
                }
            }
        }
        // output to screen
        $bind = $this->bind;
        if (($err && $this->conn['debug'] == 'err') || $this->conn['debug'] == 'all') {
            $color = ($time < 10) ? 888 : ($time > 90 ? 900 : 333);
            echo '<p style="color:#090">', nl2br(htmlspecialchars($sql), false), ' [<i style="color:#', $color ,'">', $time, ' ms</i>]</p>';
            if ($bind) {
                foreach ($bind as $k => $v) {
                    echo '<br>', $k, ' = ', htmlspecialchars($v);
                }
            }
            if ($err) {
                echo '<p style="color:#900">', nl2br($err, false), '</p>';
            }

            $bt = $err ? $bt : debug_backtrace();
            $count = 0;
            foreach ($bt as $x) {
                if (isset($x['file']) && $x['file'] != __FILE__) {
                    if (++$count == 1) {
                        echo 'L' . str_pad($x['line'], 3, 0, STR_PAD_LEFT) . ' : ' . $x['file'];
                    } elseif ($count == 2) {
                        echo ' ( ' . (isset($x['class']) ? $x['class'] : '') . ' @ ' . $x['function'] . ' )';
                        break;
                    }
                }
            }
        }
        // save err or slow SQL to log table
        if (!$this->conn['log']) {
            return;
        }

        if ($err || ($time > $this->conn['slow']
            && strpos($sql, 'INSERT INTO ' . $this->conn['log'] . '(') !== 0
            && strpos($sql, 'INSERT INTO `log_' . date('Y') . '_') !== 0
            && strpos($sql, 'INSERT INTO log_' . date('Y') . '_') !== 0 // maybe no need to log any log table ?
        )) {
            $log = $this->pdo()->prepare('INSERT INTO ' . $this->conn['log'] . '(err,url,msg) VALUES(:err,:url,:msg)');
            if (!$log) {
                echo 'config . db . log missing; ';
                return; // db error
            }
            $msg = $time . " ms\n" . $sql . "\n" . $err;
            if ($bind) {
                foreach ($bind as $k => $v) {
                    $msg .= "\n" . $k . '=' . $v;
                }
            }
            $log->execute([
                ':err' => ($err ? 'ERR' : 'SQL'),
                ':url' => ($url ? $url : $_SERVER['SCRIPT_FILENAME']),
                ':msg' => $msg . "\nIP=" . $_SERVER['REMOTE_ADDR']
            ]);
        }
    }

    private function pdo() {
        if ($this->pdo === null) {
            $this->pdo = $this->conn();
        }
        return $this->pdo;
    }

    private function selectAs($k, $v, $nl = '') {
        if (is_array($v)) {
            $kv = key($v);
            $v = reset($v);
            if (!is_numeric($kv)) {
                $k = $kv;
            }
        }
        $v = trim($v);
        return (strpos($v, ' ') && substr($v, 0, 1) != '('
                ? '('. ($nl ? "\n  " : '') . $v . ($nl ? "\n" : '') .')'
                : $this->selectKeyw($v)) . (is_numeric($k) && $k == ceil($k) ? '' : ' AS '. $k);
    }

    private function selectField($param) {
        if (!isset($param['field']) || !$param['field']) {
            return '*';
        } elseif (!is_array($param['field'])) {
            return $param['field'];
        }
        $sql = '';
        $i = 0;
        foreach ($param['field'] as $k => $v) {
            $sql .= $i++ ? ', ' : '';
            if (!$k && !is_array($v) && 'distinct' == strtolower($v)) {
                $sql .= 'DISTINCT ';
                $i--;
            } else {
                $sql .= $this->selectAs($k, $v);
            }
        }
        return $sql;
    }

    private function selectFrom($param, $glue = 1) {
        if (isset($param['table']) && !isset($param['from'])) {
            $param['from'] = $param['table'];
        }
        if (!isset($param['from']) || !$param['from']) {
            return '';
        }
        $sql = $glue ? "\nFROM " : ' ';
        if (!is_array($param['from'])) {
            return $sql . $param['from'];
        }
        $i = 0;
        foreach ($param['from'] as $k => $v) {
            $sql .= ($i++ ? ', ' : '') . $this->selectAs($k, $v, 1);
        }
        return $sql;
    }

    private function selectGroup($param) {
        if (!isset($param['group']) || !$param['group']) {
            return '';
        }
        $sql = "\nGROUP BY ";
        if (!is_array($param['group'])) {
            return $sql . $param['group'];
        }
        return $sql . implode(', ', $param['group']);
    }

    private function selectHaving($param) {
        if (!isset($param['having']) || !$param['having']) {
            return '';
        }
        $sql = "\nHAVING";
        if (!is_array($param['having'])) {
            return $sql . ' ' . $param['having'];
        }
        return $sql . $this->selectWhere($param['having'], ' AND');
    }

    private function selectJoin($param) {
        if (!isset($param['join']) || !$param['join'] || !is_array($param['join'])) {
            return '';
        }
        if (!is_array(reset($param['join']))) {
            $param['join'] = array($param['join']);
        }
        $sql = '';
        foreach ($param['join'] as $arr) { if ($arr) { // table, condition, type
            $i = 0;
            $res = []; // 0 table 1 on-condition 2 join-type
            foreach ($arr as $k => $v) {
                $res[$i++] = [$k, $v];
            }
            $sql .= "\n";
            if (isset($res[2])) {
                $type = strtoupper(substr($res[2][1], 0, 1));
                $sql .= ('L' == $type) ? 'LEFT ' : ('R' == $type ? 'RIGHT ' : ('F' == $type ? 'FULL OUTER ' : ''));
            }
            $sql .= 'JOIN ' . $this->selectAs($res[0][0], $res[0][1]);
            if (isset($res[1][1]) && $res[1][1]) {
                $sql .= ' ON' . $this->selectWhere($res[1][1], ' AND');
            }
        }}
        return $sql;
    }

    private function selectKeyw($key = '') {
        $eng = $this->conn['eng'];
        if (($eng == 'mysql' && strpos($key, '`'))
            || ($eng == 'pgsql' && strpos($key, '"'))
            || ($eng == 'mssql' && strpos($key, ']'))
            || !in_array($eng, ['mysql', 'pgsql', 'mssql'])
            // what about sqlite ? -- upgrade later
            || strpos($key, '(')
        ) {
            return $key; // already quoted
        }

        $arr = explode('.', $key);
        foreach ($arr as $i => $k) {
            //$is_int = is_numeric($k[0]);
            //$has_hyphen = (strpos($k, '-') !== false);
            //if ($is_int || $has_hyphen || in_array(strtoupper($k), $this->keyw)) {
                if ($eng == 'mysql') {
                    $arr[$i] = '`' . $k . '`';
                } elseif ($eng == 'pgsql') {
                    $arr[$i] = '"' . $k . '"';
                } elseif ($eng == 'mssql') {
                    $arr[$i] = '[' . $k . ']';
                }
            //}
        }
        return implode('.', $arr);
    }

    private function selectLimit($param = '') {
        // mssql use top X ... offset .... fix later
        // $where[limit] = 10              : LIMIT 10
        // $where[limit] = [10, 20]        : LIMIT 10 OFFSET 20
        // $where[limit] = [10, page => 2] : LIMIT 10 OFFSET 10
        if (!isset($param['limit']) || !$param['limit']) {
            return '';
        }
        $sql = "\nLIMIT ";
        if (!is_array($param['limit'])) {
            return $sql . ceil($param['limit']);
        }
        $sql .= $limit = ceil(array_shift($param['limit']));
        if (!$param['limit']) {
            return $sql;
        }
        $offset = isset($param['limit']['page']) ? (ceil($param['limit']['page']) - 1) * $limit : ceil(reset($param['limit']));
        return $sql . ($offset > 0 ? ' OFFSET ' . $offset : '');
    }

    private function selectOrder($param = '') {
        if (!isset($param['order']) || !$param['order']) {
            return '';
        }
        $sql = "\nORDER BY ";
        if (!is_array($param['order'])) {
            return $sql . $param['order'];
        }
        $i = 0;
        foreach ($param['order'] as $k => $v) {
            if (is_array($v)) {
                $k = key($v);
                $v = reset($v);
            }
            $sql .= $i++ ? ', ' : '';
            $sql .= (is_numeric($k) && $k == ceil($k))
                ? (strtoupper($v) == 'DESC' ? $k . ' DESC' : $v)
                : $this->selectKeyw($k) . (strtoupper(substr($v, 0, 1)) == 'D' ? ' DESC' : '');
        }
        return $sql;
    }

    private function selectWhere($where, $and = "\nAND") {
        if (!is_array($where)) {
            return ' ' . $where;
        }
        $sql = '';
        foreach ($where as $k => $v) {
            $sql .= strlen($sql) ? $and . ' ' : ' ';
            if (is_numeric($k) && $k == ceil($k)) { // [ ..., ..., ...]
                $op = (is_array($v) && isset($v[1]) && !is_array($v[1])) ? str_replace(['   ', '  '], ' ', trim(strtoupper($v[1]))) : '';
                if (!is_array($v)) { // 'Raw SQL'
                    $sql .= $v; // single quote ignored to bind here eg col='xyz'
                } elseif (!isset($v[1])) {  // ['Raw SQL'] or ['col', null]
                    $sql .= (count($v) > 1) ? $this->selectKeyw($v[0]) . ' IS NULL' : $v[0]; // single quote also ignored bind here
                } elseif ('NOT' == $op || '!=' == $op || '<>' == $op) {   // ['col', 'NOT', ...]
                    $sql .= $this->selectKeyw($v[0]) . (!isset($v[2]) ? ' IS NOT NULL' : $this->selectWhereKeyVal('NOT', $v[2]));
                } elseif (!isset($v[2])) {  // ['col', $val]
                    $sql .= $this->selectWhereKeyVal($v[0], ('=' == $op) ? null : $v[1]);
                } elseif (is_array($v[2])) {// ['col', op, []]
                    if ($op == '=' || $op == 'IN') {
                        $sql .= $this->selectWhereKeyVal($v[0], $v[2]);
                    } else {
                        $sql .= $this->selectKeyw($v[0]) . $this->selectWhereKeyVal('NOT', $v[2]);
                    }
                } elseif (substr($op, -7) == 'BETWEEN') { // ['col', '(not) between', $val1, $val2]
                    $sql .= $this->selectKeyw($v[0]) . ' ' . $op . ' ' . $this->selectWhereVal($v[2]) . ' AND ' . $this->selectWhereVal(isset($v[3]) ? $v[3] : null);
                } elseif ('=' == $op) { // ['col', '=', $val]
                    $sql .= $this->selectWhereKeyVal($v[0], $v[2]);
                } elseif ('IN' == $op || 'NOT IN' == $op) {// ['col', 'in',$sql]
                    $sql .= $this->selectKeyw($v[0]) . ' ' . $op . ' (' . $v[2] . ')';
                } else { // ['col', $op, $val]
                    if ($op == 'LIKE' && $v[2][0] != '%' && substr($v[2], -1) != '%') {
                        $v[2] = '%' . $v[2] . '%';
                    } elseif ($op == 'LIKE%' || $op == 'START') {
                        $v[1] = 'LIKE';
                        $v[2] .= '%';
                    } elseif ($op == '%LIKE' || $op == 'END') {
                        $v[1] = 'LIKE';
                        $v[2] = '%' . $v[2];
                    }
                    $sql .= $this->selectKeyw($v[0]) . ' ' . $v[1] . ' ' . $this->selectWhereVal($v[2]);
                }
            } else { // $k => $v, $col => $val, ...
                $sql .= $this->selectWhereKeyVal($k, $v);
            }
        }
        return $sql;
    }

    private function selectWhereKeyVal($k, $v) {
        // in, =, null, not null, exists, not exists, or
        $KK = strtoupper(trim($k));
        $KK .= ($KK == 'NOT EXIST' || $KK == 'EXIST') ? 'S' : '';
        if ($KK == 'NOT EXISTS'|| $KK == 'EXISTS') {
            return $KK . ' (' . $v . ')';
        } elseif ($KK == 'OR') {
            // please test if need trim ' OR ' instead
            return '(' . (!is_array($v) ? '1 OR ' . $v : trim($this->selectWhere($v, ' OR'))) . ')';
        }
        if (is_array($v) && count($v) == 1) {
            $v = reset($v);
        }
        if (!is_array($v)) { // please test NOT at next line
            return is_null($v) ? $this->selectKeyw($k) . ' IS NULL' : ('NOT' == $k ? ' !' : $this->selectKeyw($k) . ' ') . '= ' . $this->selectWhereVal($v);
        }
        foreach ($v as $x => $z) {
            $v[$x] = $this->selectWhereVal($z);
        }
        return ('NOT' == $k ? ' NOT ' : $this->selectKeyw($k) . ' ') . 'IN (' . implode(', ', $v) . ')';
    }

    private function selectWhereVal($v) {
        if (is_numeric($v)) {
            return is_string($v) ? "'" . $v . "'" : $v;
        } elseif (is_null($v)) {
            return 'NULL';
        } elseif (strpos($v, ';') !== false ||
            strpos($v, '#')  !== false ||
            strpos($v, '--') !== false ||
            strpos($v, '/*') !== false ||
            strpos($v, '\\') !== false
        ) {
            // only bind possible harmful SQL: # -- /* ; \'
            return $this->bindKey(0, $v); // must start with 0
        }
        return "'" . str_replace("'", "''", $v) . "'"; // i do not like bind :D
    }
}

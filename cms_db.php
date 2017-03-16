<?php
//change t #3
/**
 * Topnew CMS db v5.5.3 - Released on 2016.05.05
 * The MIT License (MIT) Copyright (c) 1998-2015, Topnew Geo, topnew.net/cms
 * A collection of php functions to handle database using PDO
 *
 * File dependency:
 * 1. cms_enc.php - if $conn['eng']['enc'] = 1
 * 2. table log_yyyy_mm needed to log error SQLs
 * 3. global var $pin['dbL'], $pin['debug'], $pin['eng'], $pin['pg_insert_id']
 *
 * conn ... [h,u,p,db,char...] . connect to database
 * debug .. show ............... toggle sql debug
 * err .... msg dbL ............ sql err msg or code
 * last_id. .................... get last insert id
 * sql|run. sql para ........... run raw-sql
 *
 * val .... sql para ........... select a value
 * row .... sql para mode ...... select a row(s) ASSOC | NUM | BOTH | KEY | 1 | 2
 * rows|all sql para mode ...... select all rows ASSOC | NUM | BOTH | KEY
 * arrX ... sql para level ..... select X level array
 * enum ... sql para ........... select array(val, val, ...)
 * list ... sql para ........... select array(key => val)
 *
 * above selects: cms_db('select', 'sql', [:key=>val]) or cms_db('select', [select, from, ...])
 *
 * insert . tab data pgLast .... eg data = [key=>val, ...]
 * replace. tab data ........... eg data = [key=>val, ...]
 * delete . tab .... where para. eg where= 'where col=val ...' OR where = [...]
 * update . tab data where para. eg data = ['col1=col2', 'col3'=>123, ...]
 * save ... tab data where para. insert | update if where found
 *
 * above where-para: string where + para[:key=>val] OR array[where] no para
 * eg where = ['col1=col2', 'col3'=>123, ...]
 * eg where = ['where' => [['col1', '=', 'col2', -1], 'col3'=>123, ...], 'order'=>..., 'limit'=>...]
 * for more example of $where please refer docs at cms_select_where()
 */
function cms_sidu($cmd = '', $tab = null, $data = null, $where = null, $para = null) {
    return cms_db($cmd, $tab, $data, $where, $para);//alias of cms_db
}
function cms_db($cmd = '', $tab = null, $data = null, $where = null, $para = null) {
    $cmd = strtolower($cmd);
    if (substr($cmd, 0, 4) === 'last') $cmd = 'last_id';//for those who forgot whether last_id or lastID
    elseif ('sql' === $cmd) $cmd = 'run';
    $func = 'cms_' . $cmd;
    if (in_array($cmd, array('conn', 'debug', 'err', 'last_id', 'run'))) return $func($tab, $data);

    if (in_array($cmd, array('val', 'row', 'rows', 'all', 'enum', 'list')) || substr($cmd, 0, 3) == 'arr') {
        if ('all' === $cmd) $cmd = 'rows';
        elseif ('enum' === $cmd || 'list' === $cmd) {
            $where = ('enum' === $cmd) ? 1 : 2;
            $cmd = 'row';
        } elseif (substr($cmd, 0, 3) == 'arr') {
            if (!$where) $where = ceil(substr($cmd, 3));//eg arr1 arr2 ...
            $cmd = 'arr';
        }
        $func = 'cms_' . $cmd;
        if (!is_array($tab)) return $func($tab, $data, $where);//sql, para, mode
        $sql = cms_select($tab, $bind);
        return $func($sql, $bind, $where);//sql, para, mode
    } //end of selects

    if (in_array($cmd, array('delete', 'update', 'save')) && is_array($where)) {
        if (isset($where['where']) && (isset($where['order']) || isset($where['limit']))) {
            $limit = cms_select_order($where) . cms_select_limit($where);
            $where = $where['where'];
        }
        $where = cms_select_where($bind, $where, ' AND');
        if ($where) $where = 'WHERE '. $where;
        if (isset($limit)) $where .= ''. $limit;
        $para  = (array)$bind;
    }
    if ('delete' == $cmd) return cms_run('DELETE FROM '. $tab ."\n". $where, $para);
    if ('save' == $cmd) $cmd = cms_val('SELECT 1 FROM '. $tab .' ' . $where . (stripos($where, 'limit ') === false ? ' LIMIT 1' : ''), $para) ? 'update' : 'insert';

    $data = cms_db_data($data, $para);
    if ('update'==$cmd) {//update support data = string eg. col1 = col2
        if ($data && is_array($data)) {
            foreach ($data as $k => $v) $data[$k] = $k .'='. $v;
            $data = implode(', ', $data);
        }
        return cms_run('UPDATE '. $tab ."\nSET ". $data ."\n". $where, $para);
    }

    if ($cmd != 'insert' && $cmd != 'replace') return;
    if (!$tab || !$data || !is_array($data)) return;
    $pg_insert_id = ($cmd != 'insert' || substr($where, 0, 8) != 'last_id:') ? '' : substr($where, 8);
    $sql = strtoupper($cmd) .' INTO '. $tab .'('. implode(', ', array_keys($data)) .")\nVALUES(". implode(', ', $data) .')';
    return cms_run($sql, $para, $pg_insert_id);
}
/* get conn[host,u,p,db,eng,enc,char,port] from $pin[conn] or /cms/setup.php */
function cms_conn($CONN = array()) {
    global $pin;
    $eng = isset($pin['eng']) ? $pin['eng'] : '';
    if (!$eng || !in_array($eng, array('pgsql', 'sqlite'))) $eng = 'mysql';
    $conn = isset($pin['conn']) ? $pin['conn'] : (function_exists('cms_conn_init') ? cms_conn_init() : array());
    $conn = isset($conn[$eng]) ? $conn[$eng] : array();
    if (!isset($conn['eng'])) $conn['eng'] = $eng;
    if (is_array($CONN)) {
        foreach ($CONN as $k => $v) $conn[$k] = $v;
    }
    if (isset($conn['enc']) && $conn['enc']) $conn['p'] = cms_dec($conn['p'], 1);
    $eng = $pin['eng'] = $conn['eng'];
    $db  = $pin['db']  = $conn['db'];
    if ('sqlite' == $eng) return new PDO('sqlite:' . $db);
    $pdo = $eng .':host=' . $conn['h'] . ';dbname=' . $db;
    $port = isset($conn['port']) ? $conn['port'] : 0;
    if (('mysql' == $eng && $port && $port<>3306) || ('pgsql' == $eng && $port && $port<>5432)) {
        $pdo .= ';port='. $port;
    }
    $options = ('mysql' == $eng && isset($conn['char']) && 'utf8' == $conn['char']) ? array(PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8') : array();
    $pin['dbL'] = new PDO($pdo, $conn['u'], $conn['p'], $options);
}
/**
 * show = all : will show any SQL on screen
 * show = err : will show any SQL that has error
 * show = -1  : will turn off cms_debug() after it is turned on
 */
function cms_debug($show = 'all') {
    global $pin;
    if (-1 == $show) $show = '';
    $pin['debug'] = $show;
}
/* Return SQL Error Code or Error Msg when msg = 1 */
function cms_err($msg = 0, $dbL = null) {
    global $pin;
    if (!isset($dbL)) $dbL = $pin['dbL'];
    $err = $dbL->errorCode();
    if (!$err || '00000' == $err) return;
    $err = $dbL->errorInfo();
    if (!$msg) return $err[1];
    return $err[1] .' '. str_replace('<', '&lt;', $err[2]);
}
function cms_last_id() {
    global $pin;//do we still need next line?
    if ('pgsql' == $pin['eng'] && isset($pin['pg_insert_id'])) return $pin['pg_insert_id'];
    return $pin['dbL']->lastInsertId();
}
function cms_run($sql = '', $para = array(), $pg_insert_id = '') {
    global $pin;
    if ('pgsql' == $pin['eng'] && $pg_insert_id) $sql .= ' returning '. $pg_insert_id;
    else $pg_insert_id = '';
    $time_start = microtime(true);
    $sth = $pin['dbL']->prepare($sql);
    if (!is_array($para)) $para = null;
    $sth->execute($para);
    $err = cms_err(1, $sth); // must before next line
    if ($pg_insert_id) {
        $row = $sth->fetch(PDO::FETCH_NUM);
        $pin['pg_insert_id'] = $row[0];
    }
    $time_end = microtime(true);
    $time = round(($time_end - $time_start) * 1000);
    if ($err) {
        $bt  = debug_backtrace();
        $err.= "\n" .'-- Debug Backtrace ----------';
        $url = '';
        foreach ($bt as $arr) {
            if (!$url && $arr['file'] <> __FILE__) $url = $arr['file'];
            $err .= "\nL" . str_pad($arr['line'], 3, 0, STR_PAD_LEFT) .': '. $arr['function'] .'() '. $arr['file'];
        }
    }
    if (isset($pin['debug']) && ($err || 'all' == $pin['debug'])) {
        $color = ($time < 10) ? 888 : ($time > 90 ? 900 : 333);
        echo '<p style="color:#090">', nl2br(htmlspecialchars($sql), false), ' [<i style="color:#', $color ,'">', $time, ' ms</i>]</p>';
        if ($para) {
            foreach ($para as $k => $v) echo '<br>', $k, ' = ', htmlspecialchars($v);
        }
        if ($err) echo '<p style="color:#900">', nl2br($err, false), '</p>';
    }
    if ($err || ($time > 1000 && substr($sql, 0, 16) <> 'INSERT INTO log_')) {
        if ($pin['eng'] != 'pgsql' && substr($sql, 0, 7) == 'INSERT ') {
            $pdo = $pin['dbL'];
            cms_conn();//prevent mysql_last_id etc | but it does not use $pin[conn] or $CONN
        }
        $log = $pin['dbL']->prepare('INSERT INTO log_err(err,url,msg) VALUES(:err,:url,:msg)');
        $msg = $time . " ms\n" . $sql . "\n" . $err;
        if ($para) {
            foreach ($para as $k => $v) $msg .= "\n" . $k .'='. $v;
        }
        $log->execute(array(':err'=>($err ? 'ERR' : 'SQL'), ':url'=>($url ? $url : $_SERVER['SCRIPT_FILENAME']), ':msg'=>$msg));
        if (isset($pdo)) $pin['dbL'] = $pdo;
    }
    return $sth;
}

/**
 * get multiple level of arrays of data from sql eg row = [a, b, c, d]
 * level=0: same as level=3 or try cms_row(,,1) quicker if one column
 * level=1: [a] = [b, c, d] or try cms_row(,,2) quicker if two columns
 * level=2: [a][b] = [c, d]
 * level=3: [a][b][c] = d
 * level=4: same as level=3
 *
 * data = [[a], [b], [c], [d]]
 * level=1: [a, b, c, d]
 *
 * if same array keys found, last value will be final, this is a good side effect when sql group is hard
 */
function cms_arr($sql, $para = array(), $level = 0) {
    if (!$sql) return array();
    $sth = cms_run($sql, $para);
    $rows= $sth->fetchAll(PDO::FETCH_ASSOC);
    if (!count($rows)) return array();
    $num = count($rows[0]);
    if (!$num) return array();//no data
    if ($level >= $num) $level = $num - 1;
    $data = array();
    if ($level < 1) {
        if ($num > 1) return $rows;
        foreach ($rows as $r) $data[] = reset($r);
        return $data;
    }
    $keys = array_slice(array_keys($rows[0]), 0, $level);
    foreach ($rows as $r) {
        $ref = &$data[$r[$keys[0]]];
        foreach ($keys as $j => $k) {
            if ($j) $ref = &$ref[$r[$k]];
            unset($r[$k]);
        }
        $ref = count($r) > 1 ? $r : reset($r);
    }
    return $data;
}
/**
 * mode=0 return one {ASSOC | NUM | BOTH} row
 * mode=1 return arr[]=row[0] -------- enum
 * mode=2 return arr[row[0]]=row[1] -- list
 * mode=k return arr[k]=row :: use cms_rows() faster
 */
function cms_row($sql = '', $para = array(), $mode = 'ASSOC') {
    if (!$sql) return array();
    if ($mode && !in_array($mode, array(1, 2, 'NUM', 'ASSOC', 'BOTH'))) {
        return cms_rows($sql, $para, $mode);
    }
    $sth = cms_run($sql, $para);
    if ('BOTH' == $mode) return $sth->fetch();
    if (!$mode || 'ASSOC' == $mode) return $sth->fetch(PDO::FETCH_ASSOC);
    if ('NUM' == $mode) return $sth->fetch(PDO::FETCH_NUM);
    $rows = $sth->fetchAll(PDO::FETCH_NUM);
    $arr  = array();
    if (1 == $mode) {/* vs cms_arr(,,0) as this always return one element array */
        foreach ($rows as $r) $arr[]=$r[0];
    } else {/* 2 vs cms_arr(,,1) as this always return one element assoc array */
        foreach ($rows as $r) $arr[$r[0]] = $r[1];
    }
    return $arr;
}
/* get rows of data from sql: {ASSOC | NUM | BOTH} */
function cms_rows($sql = '', $para = array(), $mode = 'ASSOC') {
    if (!$sql) return array();
    $sth = cms_run($sql, $para);
    if ('BOTH' == $mode) return $sth->fetchAll();
    if ('NUM'  == $mode) return $sth->fetchAll(PDO::FETCH_NUM);
    $rows = $sth->fetchAll(PDO::FETCH_ASSOC);
    if (!$mode || 'ASSOC' == $mode) return $rows;
    if (!count($rows) || !in_array($mode, array_keys($rows[0]))) return array();
    foreach ($rows as $r) $arr[$r[$mode]] = $r;//pk
    /* vs cms_arr(,,1) as this pk no need to be first col, and pk not removed from each row */
    return $arr;
}
/* get one val from sql -- $void is doing nothing */
function cms_val($sql = '', $para = array()) {
    if (!$sql) return '';
    $sth = cms_run($sql, $para);
    $row = $sth->fetch(PDO::FETCH_NUM);
    return $row[0];
}
function cms_db_data($data = null, &$para = null) {
    if (!$data || !is_array($data)) return $data;
    if (!is_array($para)) $para = array();
    foreach ($data as $k => $v) {
        if (is_null($v)) $v = 'NULL';
        elseif (!is_numeric($v) && strtoupper($v) != 'NOW()' && (!is_numeric($k) || ceil($k) != $k)) {
            //possible harmful SQL: # -- /* ; \'
            if (strpos($v, ';') || strpos($v, '#') || strpos($v, '--') || strpos($v, '/*') || strpos($v, '\\\'')) {
                $bind = ':'. $k;
                if (isset($para[$bind])) $bind .= '_'. rand();//possible bug not merely impossible
                $para[$bind] = $v;
                $v = $bind;
            } else $v = "'" . str_replace("'", "''", $v) . "'";
        }
        $data[$k] = $v;
    }
    return $data;
}
/*
$para = [
    'field' => ['distinct', 'tab1.id', 'id2' => 'tab2.id', 'num' => 'count(*)'],
    'from'  => ['tab1', 'tab2' => 'select id from tab1 where id < 100'],
    'join'  => [
        ['tab4' => 'tab1', [['tab4.id', 'like', 'tab.code', -1]], 'left'],
        ['jobs', ['id' => 56]],
    ],
    'where' => [
        'case abc when 123 then xxx else 445 end',
        'a1' => 1.23,
        'a2' => 'string',
        'a3' => '45.6',
        'a4' => [1, '2', null, 'abc'],
        'a5' => null,
        'a6' => 'null',

        ['b1', 1.23],
        ['b2', 'string'],
        ['b3', '45.6'],
        ['b4', [1, '2', null, 'abc']],
        ['b5', null],
        ['b6', 'null'],

        ['c1', 'not', 1.23],
        ['c2', 'not', 'string'],
        ['c3', 'not', '45.6'],
        ['c4', 'not', [1, '2', null, 'abc']],
        ['c5', 'not', null],
        ['c6', 'not', 'null'],
        ['c7', 'not'],
        ['c8', 'not', 'now()', -1],

        ['d1', 'between', 89, 'between-aaa'],
        ['d2', 'not between', 89, 'between-aaa'],
        ['d3', 'not between', 89],
        ['d4', 'between', 'col1', 'col2', -1],

        ['e1', 'like', 'abc%'],
        ['e2', 'not like', 'abc%'],

        ['f1', '=', 1.23],
        ['f2', '=', 'string'],
        ['f3', '=', '45.6'],
        ['f4', '=', [1, '2', null, 'abc']],
        ['f5', '=', null],
        ['f6', '=', 'null'],
        ['f7', '='],
        ['f8', '=', 'now()', -1],

        ['g1', 'in', 'sub-sql'],
        ['g2', 'in', [1, '2', null, 'abc']],

        'exist' => 'sub-sql',
        'not exist' => 'sub-sql',

        ['or', 'or-where'],
        ['or', ['x' => 1, ['y', '>', 2], 'c']],

        ['a', '>', 'abc', -1],
        ['b', '!=', '456'],

        ['heck', '=', 'a\' OR 1=1; DELETE * FROM tab; --'],
    ],
    'group' => ['abc', 'xyz', 5],
    'having'=> [['count(*)', 'aaa']],
    'order' => ['sql', 'table' => 'desc', 'dd' => 'asc', 5],
    'limit' => [2, 'page' => 20],
];
//*/
/* pass back pdo bindparams if available */
function cms_select($para = array(), &$bind = array()) {
    if (!$para) return '';
    if (!is_array($para)) return $para;
    if (isset($para['select']) && !isset($para['field'])) $para['field'] = $para['select'];
    return 'SELECT '
    . cms_select_field($para)
    . cms_select_from( $para)
    . cms_select_join( $para, $bind)
    . (isset($para['where']) && $para['where'] ? "\nWHERE" . cms_select_where($bind, $para['where']) : '')
    . cms_select_group($para)
    . cms_select_having($para,$bind)
    . cms_select_order($para)
    . cms_select_limit($para);
}
/**
 * select = * | null
 * select = distinct, col1, col2, tab.col3, alias => col4
 */
function cms_select_field($para) {
    if (!isset($para['field']) || !$para['field']) return '*';
    if (!is_array($para['field'])) return $para['field'];
    $sql = '';
    $i = 0;
    foreach ($para['field'] as $k => $v) {
        if ($i++) $sql .= ', ';
        if (!$k && 'distinct' == strtolower($v)) {
            $sql .= 'DISTINCT ';
            $i--;
        } else {
            $sql .= cms_select_as($k, $v);
        }
    }
    return $sql;
}
/* from = tab1, alias => tab2, alias3 => select * from tab5 where ... */
function cms_select_from($para) {
    if (!isset($para['from']) || !$para['from']) return '';
    $sql = "\nFROM ";
    if (!is_array($para['from'])) return $sql . $para['from'];
    $i = 0;
    foreach ($para['from'] as $k => $v) {
        if ($i++) $sql .= ', ';
        $sql .= cms_select_as($k, $v, 1);
    }
    return $sql;
}
/* join = array(array(alias=>tab, [on-where], left | right), array(join2), ...) */
function cms_select_join($para, &$bind) {
    if (!isset($para['join']) || !$para['join'] || !is_array($para['join'])) return '';
    if (!is_array(reset($para['join']))) $para['join'] = array($para['join']);
    $sql = '';
    foreach ($para['join'] as $arr) { if ($arr) {//table, condition, type
        $i = 0;
        $res = array();//0 table 1 on-condition 2 join-type
        foreach ($arr as $k => $v) {
            $res[$i++] = array($k, $v);
        }
        $sql .= "\n";
        if (isset($res[2])) {
            $type = strtoupper(substr($res[2][1], 0, 1));
            $sql .= ('L' == $type) ? 'LEFT ' : ('R' == $type ? 'RIGHT ' : ('F' == $type ? 'FULL OUTER ' : ''));
        }
        $sql .= 'JOIN '. cms_select_as($res[0][0], $res[0][1]);
        if (isset($res[1]) && $res[1]) $sql .= ' ON'. cms_select_where($bind, $res[1][1], ' AND', 0);
    }}
    return $sql;
}
function cms_select_where(&$bind, $where, $and = "\nAND", $quote = 0) {
    if (!is_array($where)) return ' '. $where;
    $i = 0;
    $sql = '';
    foreach ($where as $k => $v) {
        $sql .= $i++ ? $and .' ' : ' ';
        if (is_numeric($k) && $k == ceil($k)) {
            $op = (is_array($v) && isset($v[1]) && !is_array($v[1])) ? strtoupper($v[1]) : '';
            if (!is_array($v)) $sql .= $v;//single quote ignored here eg col='xyz'
            elseif (!isset($v[1])) $sql .= (count($v) > 1) ? cms_select_keyw($v[0]) .' IS NULL' : $v[0];//single quite also ignored
            elseif ('NOT' == $op) $sql .= cms_select_keyw($v[0]) . (!isset($v[2]) ? ' IS NOT NULL' : cms_select_where_in($bind, 'NOT', $v[2], $quote ?: (isset($v[3]) ? $v[3] : 0)));
            elseif (!isset($v[2]))   $sql .= cms_select_where_in($bind, $v[0], ('=' == $op) ? null : $v[1], $quote);
            elseif (is_array($v[2])) $sql .= cms_select_where_in($bind, $v[0], $v[2], 0);
            elseif (substr($op, -7) == 'BETWEEN') $sql .= cms_select_keyw($v[0]) .' '. $op .' '. cms_select_where_val($bind, $v[2], $quote ?: (isset($v[4]) ? $v[4] : 0)) .' AND '. cms_select_where_val($bind, isset($v[3]) ? $v[3] : null, $quote ?: (isset($v[4]) ? $v[4] : 0));
            elseif ('=' == $op) $sql .= cms_select_where_in($bind, $v[0], $v[2], $quote ?: (isset($v[3]) ? $v[3] : 0));
            elseif ('IN' == $op) $sql .= cms_select_keyw($v[0]) .' IN ('. $v[2] .')';
            else $sql .= cms_select_keyw($v[0]) .' '. $v[1] .' '. cms_select_where_val($bind, $v[2], $quote ?: (isset($v[3]) ? $v[3] : 0));
        } else $sql .= cms_select_where_in($bind, $k, $v, $quote);
    }
    return $sql;
}
/* in, =, null, not null, exists, not exists, or */
function cms_select_where_in(&$bind, $k, $v, $quote) {
    $KK = strtoupper(trim($k));
    if ($KK == 'NOT EXIST' || $KK == 'EXIST' ) $KK .= 'S';
    if ($KK == 'NOT EXISTS'|| $KK == 'EXISTS') return $KK .' ('. $v .')';
    if ($KK == 'OR') {
        if (!is_array($v)) return '(1 OR '. $v .')';//quote not escaped here
        return '('. trim(cms_select_where($bind, $v, ' OR')) .')';
    }
    if (is_array($v)) {
        foreach ($v as $x => $z) $v[$x] = cms_select_where_val($bind, $z);
        return ('NOT' == $k ? ' NOT ' : $k .' ') .'IN ('. implode(', ', $v) .')';
    }
    $VV = strtoupper(trim($v));
    if (is_null($v)) return $k .' IS NULL';
    return ('NOT' == $k ? ' !' : $k .' ') .'= '. cms_select_where_val($bind, $v, $quote);
}
function cms_select_where_val(&$bind, $v, $quote = 0) {
    if ($quote < 0) return $v;
    if (is_numeric($v)) {
        if (is_string($v)) return "'" . $v . "'";
        return $v;
    }
    if (is_null($v)) return 'NULL';
    //possible harmful SQL: # -- /* ; \'
    if (strpos($v, ';') || strpos($v, '#') || strpos($v, '--') || strpos($v, '/*') || strpos($v, '\\\'')) {
        $k = count($bind);
        $bind[':'. $k] = $v;
        return ':'. $k;
    }
    return "'" . str_replace("'", "''", $v) . "'";//i do not like bind
}
/* group = col1, col2, 3, 5*/
function cms_select_group($para) {
    if (!isset($para['group']) || !$para['group']) return '';
    $sql = "\nGROUP BY ";
    if (!is_array($para['group'])) return $sql . $para['group'];
    return $sql . implode(', ', $para['group']);
}
/* having = col operand value */
function cms_select_having($para, &$bind) {
    if (!isset($para['having']) || !$para['having']) return '';
    $sql = "\nHAVING";
    if (!is_array($para['having'])) return $sql .' '. $para['having'];
    return $sql . cms_select_where($bind, $para['having'], ' AND');
}
/* order = a, 2, col => DESC */
function cms_select_order($para) {
    if (!isset($para['order']) || !$para['order']) return '';
    $sql = "\nORDER BY ";
    if (!is_array($para['order'])) return $sql . cms_select_keyw($para['order']);
    $i = 0;
    foreach ($para['order'] as $k => $v) {
        if ($i++) $sql .= ', ';
        if (is_numeric($k) && $k == ceil($k)) $sql .= $v;
        else $sql .= cms_select_keyw($k) . ('D' == strtoupper(substr($v, 0, 1)) ? ' DESC' : '');
    }
    return $sql;
}
/**
 * limit = 10
 * limit = 10, 20 : limit 10 offset 20
 * limit = 10, page => 2 : limit 10 offset 20
 */
function cms_select_limit($para) {
    if (!isset($para['limit']) || !$para['limit']) return '';
    $sql = "\nLIMIT ";
    if (!is_array($para['limit'])) return $sql . ceil($para['limit']);
    $sql .= $limit = ceil(array_shift($para['limit']));
    if (!$para['limit']) return $sql;
    $sql .= ' OFFSET ';
    if (isset($para['limit']['page'])) return $sql . (ceil($para['limit']['page']) - 1) * $limit;
    return $sql . ceil(array_shift($para['limit']));
}
function cms_select_as($k, $v, $nl = '') {
    $v = trim($v);
    return (strpos($v, ' ') && substr($v, 0, 1) != '(' ?
        '('. ($nl ? "\n  " : '') . $v . ($nl ? "\n" : '') .')' : cms_select_keyw($v))
        . (is_numeric($k) && $k == ceil($k) ? '' : ' AS '. $k);
}
function cms_select_keyw($k) {
    //adding more sql keywords -- fix later
    if (in_array($k, array('select', 'table', 'sql'))) return '`'. $k .'`';
    return $k;
}

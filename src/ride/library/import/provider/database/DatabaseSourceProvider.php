<?php

namespace ride\library\import\provider\database;

use ride\library\import\exception\ImportException;
use ride\library\import\provider\SourceProvider;
use ride\library\import\Importer;

/**
 * Import source provider of a ORM model
 */
class DatabaseSourceProvider extends AbstractDatabaseProvider implements SourceProvider {

    /**
     * SQL statement
     * @var string
     */
    protected $sql;

    /**
     * Sets the table to use, column names will be queried
     * @param string $table Name of the table
     * @return null
     */
    public function setTable($table, $limit = 0) {
        $this->sql = null;
        $this->limit = $limit;

        parent::setTable($table);
    }

    /**
     * Sets the SQL to use accompanied with the available column names
     * @param string $sql SQL statement
     * @param array $columnNames Array with the available column names
     * @return null
     */
    public function setSql($sql, array $columnNames) {
        $this->table = null;
        $this->sql = $sql;
        $this->columnNames = array();

        foreach ($columnNames as $columnName) {
            $this->columnNames[$columnName] = $columnName;
        }
    }

    /**
     * Performs preparation tasks of the import
     * @return null
     */
    public function preImport(Importer $importer) {
        if ($this->sql) {
            $sql = $this->sql;
        } elseif ($this->table) {
            $sql = 'SELECT * FROM ' . $this->connection->quoteIdentifier($this->table);

            if ($this->limit) {
                $sql .= ' LIMIT ' . $this->limit;
            }
        } else {
            throw new ImportException('Could not execute source query, no table of sql set');
        }

        $this->result = $this->connection->execute($sql)->getRows();
        reset($this->result);
    }

    /**
     * Gets the next row from this destination
     * @return array|null $data Array with the name of the column as key and the
     * value to import as value. Null is returned when all rows are processed.
     */
    public function getRow() {
        $row = each($this->result);
        if ($row === false) {
            return null;
        }

        $row = $row['value'];

        $result = array();
        foreach ($this->columnNames as $columnName) {
            $result[$columnName] = $this->reflectionHelper->getProperty($row, $columnName);
        }

        return $result;
    }

    /**
     * Performs finishing tasks of the import
     * @return null
     */
    public function postImport() {
        $this->result = null;
    }

}

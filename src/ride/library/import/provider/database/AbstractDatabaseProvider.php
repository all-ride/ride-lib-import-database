<?php

namespace ride\library\import\provider\database;

use ride\library\database\DatabaseManager;
use ride\library\import\exception\ImportException;
use ride\library\import\provider\Provider;
use ride\library\import\Importer;
use ride\library\reflection\ReflectionHelper;

/**
 * Abstract import provider for data from a database
 */
class AbstractDatabaseProvider implements Provider {

    /**
     * Instance of the database manager
     * @var \ride\library\database\DatabaseManager
     */
    protected $databaseManager;

    /**
     * Instance of a database connection
     * @var \ride\library\database\driver\Driver
     */
    protected $connection;

    /**
     * Name of the table
     * @var string
     */
    protected $table;

    /**
     * Instance of the reflection helper
     * @var \ride\library\reflection\ReflectionHelper
     */
    protected $reflectionHelper;

    /**
     * Array with the name of the column as key and value
     * @var array
     */
    protected $columnNames;

    /**
     * Constructs a new database provider
     * @return null
     */
    public function __construct(DatabaseManager $databaseManager, ReflectionHelper $reflectionHelper, $connction = null) {
        $this->databaseManager = $databaseManager;
        $this->connection = $this->databaseManager->getConnection($connection);
        $this->reflectionHelper = $reflectionHelper;
    }

    /**
     * Sets the table to use, column names will be queried
     * @param string $table Name of the table
     * @return null
     */
    public function setTable($table) {
        $this->table = $table;

        $definer = $this->databaseManager->getDefiner($this->connection);
        $table = $definer->getTable($table);

        $fields = $table->getFields();
        foreach ($fields as $fieldName => $field) {
            $this->columnNames[$fieldName] = $fieldName;
        }
    }

    /**
     * Gets the available column names for this provider
     * @return array Array with the name of the column as key and as value
     */
    public function getColumnNames() {
        if ($this->columnNames === null) {
            throw new ImportException('Could not get the column names, initialize this provider with a table or query');
        }

        return $this->columnNames;
    }

    /**
     * Performs preparation tasks of the import
     * @return null
     */
    public function preImport(Importer $importer) {

    }

    /**
     * Performs finishing tasks of the import
     * @return null
     */
    public function postImport() {

    }

}

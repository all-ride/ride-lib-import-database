<?php

namespace ride\library\import\provider\database;

use ride\library\database\manipulation\expression\FieldExpression;
use ride\library\database\manipulation\expression\TableExpression;
use ride\library\database\manipulation\statement\InsertStatement;
use ride\library\import\exception\ImportException;
use ride\library\import\provider\DestinationProvider;
use ride\library\import\Importer;

use \Exception;

/**
 * Import destination provider of a database table
 */
class DatabaseDestinationProvider extends AbstractOrmProvider implements DestinationProvider {

    /**
     * Flag to see if this import started the transaction
     * @var boolean
     */
    protected $isTransactionStarted;

    /**
     * Performs preparation tasks of the import
     * @return null
     */
    public function preImport(Importer $importer) {
        if (!$table) {
            throw new ImportException('Could not initialize insert query, no table set');
        }

        $this->isTransactionStarted = $this->connection->beginTransaction();

        $this->statement = new InsertStatement();
        $this->statement->addTable(new TableExpression($this->table));
    }

    /**
     * Imports a row into this destination
     * @param array $row Array with the name of the column as key and the
     * value to import as value
     */
    public function setRow(array $row) {
        $statement = clone $this->statement;

        try {
            foreach ($this->columnNames as $columnName) {
                if (isset($row[$columnName])) {
                    $statement->addValue(new FieldExpression($columnName), $row[$columnName]);
                }
            }

            $this->connection->executeStatement($statement);
        } catch (Exception $exception) {
            if ($this->isTransactionStarted) {
                $this->connection->rollbackTransaction();
            }

            throw $exception;
        }
    }

    /**
     * Performs finishing tasks of the import
     * @return null
     */
    public function postImport() {
        if ($this->isTransactionStarted) {
            $this->connection->commitTransaction();
        }
    }

}

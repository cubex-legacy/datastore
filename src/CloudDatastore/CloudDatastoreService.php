<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore;

use api\services\datastore\BeginTransactionRequest;
use api\services\datastore\BlindWriteRequest;
use api\services\datastore\CommitRequest;
use api\services\datastore\Entity;
use api\services\datastore\Key;
use api\services\datastore\LookupRequest;
use api\services\datastore\Mutation;
use api\services\datastore\PartitionId;
use api\services\datastore\Query;
use api\services\datastore\QueryResultBatch;
use api\services\datastore\ReadOptions;
use api\services\datastore\RollbackRequest;
use api\services\datastore\RunQueryRequest;
use GoogleAPI\GoogleServiceOptions;
use Cubex\ServiceManager\IService;
use Cubex\ServiceManager\ServiceConfigTrait;

class CloudDatastoreService implements IService
{
  const DEFAULT_HOST        = 'https://www.googleapis.com';
  const DEFAULT_PK_PASSWORD = 'notasecret';

  use ServiceConfigTrait;

  /**
   * @var Connection
   */
  protected $_connection;
  /**
   * @var string
   */
  protected $_namespace;
  /**
   * @var string
   */
  protected $_transactionId = "";
  /**
   * @var Mutation
   */
  protected $_currentMutation = null;


  public function connect()
  {
    $dataSet = $this->config()->getStr("dataset");
    if(!$dataSet)
    {
      throw new \Exception('CloudDatastore: dataset not configured');
    }
    $options = new GoogleServiceOptions();
    $options->setHost($this->config()->getStr("host", self::DEFAULT_HOST));
    $options->setPrivateKeyFile($this->config()->getStr("privateKeyFile", ""));
    $options->setPrivateKeyPassword(
      $this->config()->getStr("privateKeyPassword", self::DEFAULT_PK_PASSWORD)
    );
    $options->setServiceAccountName(
      $this->config()->getStr("serviceAccount", "")
    );
    $options->setApplicationName(
      $this->config()->getStr("applicationName", "")
    );
    $options->setClientId($this->config()->getStr("clientId", ""));

    $this->_connection = new Connection($options);
    $this->_connection->setDataset($dataSet);
    $this->_namespace = $this->config()->getStr("namespace", "");
  }

  public function disconnect()
  {
    $this->_connection = null;
  }

  /**
   * @return Connection
   */
  public function conn()
  {
    if($this->_connection === null)
    {
      $this->connect();
    }
    return $this->_connection;
  }

  /**
   * @param Key $key
   *
   * @return string
   */
  public function encodeKey(Key $key)
  {
    return base64_encode($key->serialize());
  }

  /**
   * @param string $keyStr
   *
   * @return Key
   */
  public function decodeKey($keyStr)
  {
    $key = new Key();
    $key->parse(base64_decode($keyStr));
    return $key;
  }

  public function keysMatch(Key $keyA, Key $keyB)
  {
    return $this->encodeKey($keyA) == $this->encodeKey($keyB);
  }

  /**
   * Begin a transaction
   *
   * @return string The transaction ID
   * @throws \Exception
   */
  public function beginTransaction()
  {
    if($this->inTransaction())
    {
      throw new \Exception(
        'Already in a transaction. Please commit or rollback before starting a new transaction.'
      );
    }

    $req = new BeginTransactionRequest();
    $resp = $this->conn()->beginTransaction($req);
    $this->_transactionId = $resp->getTransaction();

    $this->_currentMutation = new Mutation();

    return $this->_transactionId;
  }

  public function inTransaction()
  {
    return $this->_transactionId ? true : false;
  }

  /**
   * @return null|Key[] AutoInsertId keys if any were created
   * @throws \Exception
   */
  public function commit()
  {
    if(! $this->inTransaction())
    {
      throw new \Exception('Not in a transaction');
    }

    // TODO: retries on timeouts?
    $req = new CommitRequest();
    $req->setTransaction($this->_transactionId);
    $req->setMutation($this->_currentMutation);
    $resp = $this->conn()->commit($req);

    $this->_transactionId = "";
    $this->_currentMutation = null;

    $mutationResult = $resp->getMutationResult();
    if($mutationResult->hasInsertAutoIdKey())
    {
      $autoIdKeys = $mutationResult->getInsertAutoIdKeyList();
    }
    else
    {
      $autoIdKeys = null;
    }
    return $autoIdKeys;
  }

  public function rollback()
  {
    if(! $this->inTransaction())
    {
      throw new \Exception('Not in a transaction');
    }

    $req = new RollbackRequest();
    $req->setTransaction($this->_transactionId);
    $this->_transactionId = "";
    $this->_currentMutation = null;
    $this->conn()->rollback($req);
    // TODO: Do something with the response??
  }

  /**
   * @param string  $kind
   * @param string  $name
   * @param array[] $ancestorPath An array where each element is an array
   *                              containing 'kind' and either 'id' or 'name'
   *
   * @return Entity
   */
  public function getEntityByName($kind, $name, $ancestorPath = null)
  {
    $path = $ancestorPath ? $ancestorPath : [];
    $path[] = ['kind' => $kind, 'name' => $name];
    return $this->getEntityByPath($path);
  }

  /**
   * @param array $path
   *
   * @return Entity
   */
  public function getEntityByPath($path)
  {
    return $this->getEntity($this->makeKeyFromPath($path));
  }


  public function getEntitiesByName($kind, $names, $ancestorPath = null)
  {
    $paths = [];
    foreach($names as $name)
    {
      $paths[] = ['kind' => $kind, 'name' => $name];
    }
    return $this->getEntitiesByPath($paths, $ancestorPath);
  }

  public function getEntitiesById($kind, $ids, $ancestorPath = null)
  {
    $paths = [];
    foreach($ids as $id)
    {
      $paths[] = ['kind' => $kind, 'id' => $id];
    }
    return $this->getEntitiesByPath($paths, $ancestorPath);
  }

  /**
   * @param Key $key
   *
   * @return Entity
   */
  public function getEntity(Key $key)
  {
    $res = $this->getEntities([$key]);
    reset($res);
    return current($res);
  }

  /**
   * Get entities by their kind and id/name
   *
   * @param array[]      $childPaths The final path element for each element.
   *                                 This should be an array of arrays
   *                                 where each contains 'kind' and
   *                                 either 'id' or 'name'
   * @param null|array[] $ancestorPath
   *
   * @return Entity[]
   */
  public function getEntitiesByPath($childPaths, $ancestorPath = null)
  {
    $keys = [];
    foreach($childPaths as $childPath)
    {
      $fullPath = $ancestorPath ? $ancestorPath : [];
      $fullPath[] = $childPath;
      $keys[] = $this->makeKeyFromPath($fullPath);
    }

    return $this->getEntities($keys);
  }

  /**
   * Run a query and return the results
   *
   * @param Query  $query
   *
   * @return Entity[]
   */
  public function runQuery(Query $query)
  {
    $entities = [];

    $req = new RunQueryRequest();
    $req->setQuery($query);
    if($this->_namespace != "")
    {
      $req->setPartitionId($this->_getPartitionId());
    }
    if($this->inTransaction())
    {
      $req->setReadOptions($this->_getTransactionReadOptions());
    }

    $response = $this->conn()->runQuery($req);
    $entityResults = $response->getBatch()->getEntityResultList();
    foreach($entityResults as $result)
    {
      $entities[] = $result->getEntity();
    }
    return $entities;
  }

  /**
   * Perform a lookup with retries
   *
   * @param Key[]  $keys
   * @param int    $retryLimit Maximum number of retries
   * @param int    $retryNum Internal use: The current retry number
   *
   * @return Entity[] The returned objects, the array keys
   *                  are the encoded Key objects
   * @throws \Exception
   */
  public function getEntities($keys, $retryLimit = 5, $retryNum = 0)
  {
    $result = [];
    $req = new LookupRequest();
    foreach($keys as $key)
    {
      $req->addKey($key);
    }

    if($this->inTransaction())
    {
      $req->setReadOptions($this->_getTransactionReadOptions());
    }

    $response = $this->conn()->lookup($req);
    if($response->hasFound())
    {
      foreach($response->getFoundList() as $entityResult)
      {
        $entity = $entityResult->getEntity();
        $keyStr = $this->encodeKey($entity->getKey());
        $result[$keyStr] = $entity;
      }
    }

    if($response->hasMissing())
    {
      foreach($response->getMissingList() as $entityResult)
      {
        $key = $entityResult->getEntity()->getKey();
        $keyStr = $this->encodeKey($key);
        $result[$keyStr] = null;
      }
    }

    if($response->hasDeferred())
    {
      if($retryNum < $retryLimit)
      {
        $retryNum++;
        sleep($retryNum * 2);
        $retryRes = $this->getEntities(
          $response->getDeferredList(),
          $retryLimit,
          $retryNum
        );
        $result = $result + $retryRes;
      }
      else
      {
        throw new \Exception(
          'Retry limit hit when performing Datastore lookup'
        );
      }
    }

    return $result;
  }

  /**
   * Write a single entity. If a transaction is open the write
   * will be added to the transaction, otherwise it will be performed
   * using a BlindWrite operation
   *
   * @param Entity $entity
   */
  public function writeEntity(Entity $entity)
  {
    $this->writeEntities([$entity], false);
  }

  /**
   * Write multiple entities. If a transaction is open the
   * write will be added to the transaction, otherwise it will
   * be performed using a BlindWrite operation
   *
   * @param Entity[] $entities
   * @param bool     $useTransaction
   *
   * @throws \Exception
   */
  public function writeEntities($entities, $useTransaction = true)
  {
    if($this->inTransaction())
    {
      foreach($entities as $entity)
      {
        $this->_currentMutation->addUpsert($entity);
      }
    }
    else if($useTransaction)
    {
      $committing = false;
      $this->beginTransaction();
      try
      {
        foreach($entities as $entity)
        {
          $this->_currentMutation->addUpsert($entity);
        }
        $committing = true;
        $this->commit();
      }
      catch(\Exception $e)
      {
        if(! $committing)
        {
          $this->rollback();
        }
        throw $e;
      }
    }
    else
    {
      $mutation = new Mutation();
      foreach($entities as $entity)
      {
        $mutation->addUpsert($entity);
      }

      $this->_performBlindWrite($mutation);
    }
  }

  public function insertAutoId($entity)
  {
    $keys = $this->insertAutoIdMulti([$entity], false);
    return $keys ? $keys[0] : null;
  }

  /**
   * Insert multiple entities and auto-generate the ID
   *
   * @param Entity[] $entities
   * @param bool     $useTransaction If true then perform the operation in a
   *                                 transaction. Ignored if a transaction
   *                                 is already in progress.
   *
   * @return null|Key[] null if in a transaction, or the new keys if not
   * @throws \Exception
   */
  public function insertAutoIdMulti($entities, $useTransaction = true)
  {
    if($this->inTransaction())
    {
      foreach($entities as $entity)
      {
        $this->_currentMutation->addInsertAutoId($entity);
      }
      return null;
    }
    else if($useTransaction)
    {
      $committing = false;
      $this->beginTransaction();
      try
      {
        foreach($entities as $entity)
        {
          $this->_currentMutation->addInsertAutoId($entity);
        }
        $committing = true;
        return $this->commit();
      }
      catch(\Exception $e)
      {
        if(! $committing)
        {
          $this->rollback();
        }
        throw $e;
      }
    }
    else
    {
      $mutation = new Mutation();
      foreach($entities as $entity)
      {
        $mutation->addInsertAutoId($entity);
      }
      $resp = $this->_performBlindWrite($mutation);
      return $resp->getMutationResult()->getInsertAutoIdKeyList();
    }
  }

  /**
   * Delete an entity
   *
   * @param Key $key
   */
  public function delete(Key $key)
  {
    $this->deleteMulti([$key], false);
  }

  /**
   * Delete multiple entities
   *
   * @param Key[] $keys
   * @param bool  $useTransaction
   *
   * @throws \Exception
   */
  public function deleteMulti($keys, $useTransaction = true)
  {
    if($this->inTransaction())
    {
      foreach($keys as $key)
      {
        $this->_currentMutation->addDelete($key);
      }
    }
    else if($useTransaction)
    {
      $committing = false;
      $this->beginTransaction();
      try
      {
        foreach($keys as $key)
        {
          $this->_currentMutation->addDelete($key);
        }
        $committing = true;
        $this->commit();
      }
      catch(\Exception $e)
      {
        if(! $committing)
        {
          $this->rollback();
        }
        throw $e;
      }
    }
    else
    {
      $mutation = new Mutation();
      foreach($keys as $key)
      {
        $mutation->addDelete($key);
      }
      $this->_performBlindWrite($mutation);
    }
  }

  protected function _performBlindWrite(Mutation $mutation)
  {
    // TODO: retries on timeouts?
    $req = new BlindWriteRequest();
    $req->setMutation($mutation);
    return $this->conn()->blindWrite($req);
  }


  public function makeKeyFromPath($path)
  {
    $key = new Key();
    foreach($path as $pathElement)
    {
      $pe = new Key\PathElement();
      $pe->setKind($pathElement['kind']);
      if(isset($pathElement['name']))
      {
        $pe->setName($pathElement['name']);
      }
      else if(isset($pathElement['id']))
      {
        $pe->setId($pathElement['id']);
      }
      $key->addPathElement($pe);
    }
    if($this->_namespace != "")
    {
      $key->setPartitionId($this->_getPartitionId());
    }
    return $key;
  }

  protected function _getPartitionId()
  {
    return (new PartitionId())->setNamespace($this->_namespace);
  }

  protected function _getTransactionReadOptions()
  {
    return (new ReadOptions())->setTransaction($this->_transactionId);
  }
}

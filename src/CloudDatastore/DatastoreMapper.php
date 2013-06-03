<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore;

use api\services\datastore\Entity;
use api\services\datastore\Key;
use api\services\datastore\Property;
use api\services\datastore\Value;
use CloudDatastore\Facade\CloudDatastore;
use Cubex\Mapper\DataMapper;

class DatastoreMapper extends DataMapper
{
  protected $_autoTimestamp = false;
  protected $_attributeType = '\CloudDatastore\DatastoreAttribute';

  /**
   * The name of the datastore service to use
   * @var string
   */
  protected $_datastoreConnection = 'datastore';
  /**
   * The default ancestor path to use for entities of this type.
   * This should be an array of path elements where each element is an array
   * with fields 'kind' and either 'id' or 'name', e.g.
   * [['kind' => 'TestElement', 'name' => 'ParentElement'] ... ]
   * @var null|array
   */
  protected $_defaultAncestorPath = null;
  /**
   * If this is true then the ID is a string used as the Name property.
   * If it is false then ID is an integer used as the Datastore ID.
   * @var bool
   */
  protected $_idIsName = true;
  /**
   * The name of the Kind represented by this mapper
   * @var string
   */
  protected $_kind;
  /**
   * The actual ancestor path of the object. This may or may not be the
   * same as the defaultAncestorPath above.
   * @var null|array
   */
  protected $_ancestorPath = null;
  /**
   * The actual Entity
   * @var Entity
   */
  protected $_entity = null;
  /**
   * The entity's key
   * @var Key
   */
  protected $_key = null;

  public function connection()
  {
    return CloudDatastore::getAccessor($this->_datastoreConnection);
  }

  public function __construct($id = null, $ancestorPath = null)
  {
    if(empty($this->_kind))
    {
      throw new \Exception('ERROR: Mapper does not specify a Kind');
    }

    parent::__construct($id);

    if($ancestorPath === null)
    {
      $ancestorPath = $this->_defaultAncestorPath;
    }
    $this->setAncestorPath($ancestorPath);

    if($id !== null)
    {
      $this->load($id, $ancestorPath);
    }
  }

  public function kind()
  {
    return $this->_kind;
  }

  public function setAncestorPath($ancestorPath)
  {
    $this->_pathChanged();
    $this->_ancestorPath = $ancestorPath;
    return $this;
  }

  public function ancestorPath()
  {
    return $this->_ancestorPath;
  }

  public function setId($id)
  {
    $this->_pathChanged();
    return parent::setId($id);
  }

  protected function _pathChanged()
  {
    $this->_key = null;
  }

  public function key()
  {
    if($this->_key === null)
    {
      $this->_key = $this->connection()->makeKeyFromPath(
        $this->_getEntityPath()
      );
    }
    return $this->_key;
  }

  public function entity()
  {
    if($this->_entity === null)
    {
      $this->load($this->id(), $this->ancestorPath());
    }
    return $this->_entity;
  }

  public function load($id = null, $ancestorPath = null)
  {
    if($ancestorPath === null)
    {
      $ancestorPath = $this->_defaultAncestorPath;
    }
    $this->setAncestorPath($ancestorPath);
    $this->setId($id);

    $this->_entity = $this->connection()->getEntity($this->key());
    $this->hydrateFromEntity($this->_entity);
  }

  public function delete()
  {
    $this->connection()->delete($this->key());
  }

  public function saveChanges(
    $validate = false, $processAll = false, $failFirst = false
  )
  {
    $this->_saveValidation(
      $validate,
      $processAll,
      $failFirst
    );

    $entity = new Entity();
    $entity->setKey($this->key());
    foreach($this->getRawAttributes() as $attribute)
    {
      if($attribute instanceof DatastoreAttribute)
      {
        $value = new Value();
        $data = $attribute->rawData();

        switch($attribute->type())
        {
          case DatastoreAttribute::TYPE_STRING:
            $value->setStringValue($data);
            break;
          case DatastoreAttribute::TYPE_INT:
            $value->setIntegerValue($data);
            break;
          case DatastoreAttribute::TYPE_BOOL:
            $value->setBooleanValue($data);
            break;
          case DatastoreAttribute::TYPE_DOUBLE:
            $value->setDoubleValue($data);
            break;
          case DatastoreAttribute::TYPE_TIMESTAMP:
            $value->setTimestampMicrosecondsValue($data);
            break;
          case DatastoreAttribute::TYPE_BLOB_KEY:
            $value->setBlobKeyValue($data);
            break;
          case DatastoreAttribute::TYPE_BLOB:
            $value->setBlobValue($data);
            break;
          case DatastoreAttribute::TYPE_ENTITY:
            $value->setEntityValue($data);
            break;
          case DatastoreAttribute::TYPE_KEY:
            $value->setKeyValue($data);
            break;
        }

        $entity->addProperty(
          (new Property())
            ->setName($attribute->name())
            ->setMulti($attribute->multi())
            ->setValue($value)
        );
      }
    }

    $this->connection()->writeEntity($entity);
    $this->_entity = $entity;
  }

  public function setEntity(Entity $entity)
  {
    $this->_entity = $entity;
    $this->hydrateFromEntity($entity);
  }

  public function hydrateFromEntity(Entity $entity)
  {
    $this->hydrate($this->connection()->entityToArray($entity));
  }

  protected function _getEntityPath()
  {
    $path = $this->_ancestorPath ? $this->_ancestorPath : [];

    $finalPath = ['kind' => $this->kind()];
    if($this->_idIsName)
    {
      $finalPath['name'] = $this->id();
    }
    else
    {
      $finalPath['id'] = $this->id();
    }
    $path[] = $finalPath;
    return $path;
  }
}

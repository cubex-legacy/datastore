<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore\Facade;

class CloudDatastore extends BaseFacade
{
  /**
   * @param string $name
   *
   * @return \CloudDatastore\CloudDatastoreService
   */
  public static function getAccessor($name = 'datastore')
  {
    return static::getServiceManager()->get($name);
  }
}

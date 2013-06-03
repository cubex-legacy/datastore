<?php
/**
 * @author  Richard.Gooding
 */

namespace CloudDatastore\Facade;

use Cubex\Facade\BaseFacade;

class CloudDatastore extends BaseFacade
{
  /**
   * @param string $name
   *
   * @return \CloudDatastore\DatastoreService
   */
  public static function getAccessor($name = 'datastore')
  {
    return static::getServiceManager()->get($name);
  }
}

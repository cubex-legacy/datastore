<?php
/**
 * @author  Richard.Gooding
 */

namespace Cubex\KvStore\CloudDatastore\GoogleAPI;

class GoogleServiceOptions
{
  public $host = 'https://www.googleapis.com';
  public $applicationName = '';
  public $clientId = '';
  public $serviceAccountName = '';
  public $privateKeyFile;
  public $privateKeyPassword = 'notasecret';
  public $authTokenFile;

  public function getHost()
  {
    return $this->host;
  }

  public function setHost($host)
  {
    $this->host = $host;
    return $this;
  }

  public function getApplicationName()
  {
    return $this->applicationName;
  }

  public function setApplicationName($applicationName)
  {
    $this->applicationName = $applicationName;
    return $this;
  }


  public function getClientId()
  {
    return $this->clientId;
  }

  public function setClientId($clientId)
  {
    $this->clientId = $clientId;
    return $this;
  }

  public function getServiceAccountName()
  {
    return $this->serviceAccountName;
  }

  public function setServiceAccountName($serviceAccountName)
  {
    $this->serviceAccountName = $serviceAccountName;
    return $this;
  }

  public function getPrivateKeyFile()
  {
    return $this->privateKeyFile;
  }

  public function setPrivateKeyFile($privateKeyFile)
  {
    $this->privateKeyFile = $privateKeyFile;
    return $this;
  }

  public function setPrivateKeyPassword($password)
  {
    $this->privateKeyPassword = $password;
    return $this;
  }

  public function getAuthTokenFile()
  {
    return $this->authTokenFile;
  }

  public function setAuthTokenFile($authTokenFile)
  {
    $this->authTokenFile = $authTokenFile;
    return $this;
  }
}

<?php

/*
 * RakLib network library
 *
 *
 * This project is not affiliated with Jenkins Software LLC nor RakNet.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 */

namespace raklib\server;

use raklib\utils\Logger;


class RakLibServer extends \pmmp\thread\Thread{
    protected $port;
    protected $interface;
    /** @var Logger */
    protected $logger;
    /** @var string */
    protected $composerAutoloaderPath;

    protected $shutdown;

    /** @var \Threaded */
    protected $externalQueue;
    /** @var \Threaded */
    protected $internalQueue;

	protected $mainPath;

	/**
	 * @param Logger          $logger
	 * @param string          $composerAutoloaderPath
	 * @param int             $port
	 * @param string          $interface
	 *
	 * @throws \Exception
	 */
    public function __construct(Logger $logger, $composerAutoloaderPath, $port, $interface = "0.0.0.0"){
        $this->port = (int) $port;
        if($port < 1 or $port > 65536){
            throw new \Exception("Invalid port range");
        }

        $this->interface = $interface;
        $this->logger = $logger;
        $this->composerAutoloaderPath = $composerAutoloaderPath;
        $this->shutdown = false;

        $this->externalQueue = new \pmmp\thread\ThreadSafeArray;
        $this->internalQueue = new \pmmp\thread\ThreadSafeArray;

	    if(\Phar::running(true) !== ""){
		    $this->mainPath = \Phar::running(true);
	    }else{
		    $this->mainPath = \getcwd() . DIRECTORY_SEPARATOR;
	    }
        $this->start(\pmmp\thread\Thread::INHERIT_NONE);
    }


    public function isShutdown(){
        return $this->shutdown === true;
    }

    public function shutdown(){
        $this->shutdown = true;
    }

    public function getPort(){
        return $this->port;
    }

    public function getInterface(){
        return $this->interface;
    }

    /**
     * @return Logger
     */
    public function getLogger(){
        return $this->logger;
    }

    /**
     * @return \Threaded
     */
    public function getExternalQueue(){
        return $this->externalQueue;
    }

    /**
     * @return \Threaded
     */
    public function getInternalQueue(){
        return $this->internalQueue;
    }

    public function pushMainToThreadPacket($str){
        $this->internalQueue[] = $str;
    }

    public function readMainToThreadPacket(){
        return $this->internalQueue->shift();
    }

    public function pushThreadToMainPacket($str){
        $this->externalQueue[] = $str;
    }

    public function readThreadToMainPacket(){
        return $this->externalQueue->shift();
    }

	public function shutdownHandler(){
		if($this->shutdown !== true){
			$this->getLogger()->emergency("RakLib crashed!");
		}
	}

	public function errorHandler($errno, $errstr, $errfile, $errline, $trace = null){
		if(error_reporting() === 0){
			return false;
		}
		$errorConversion = [
			E_ERROR => "E_ERROR",
			E_WARNING => "E_WARNING",
			E_PARSE => "E_PARSE",
			E_NOTICE => "E_NOTICE",
			E_CORE_ERROR => "E_CORE_ERROR",
			E_CORE_WARNING => "E_CORE_WARNING",
			E_COMPILE_ERROR => "E_COMPILE_ERROR",
			E_COMPILE_WARNING => "E_COMPILE_WARNING",
			E_USER_ERROR => "E_USER_ERROR",
			E_USER_WARNING => "E_USER_WARNING",
			E_USER_NOTICE => "E_USER_NOTICE",
			E_STRICT => "E_STRICT",
			E_RECOVERABLE_ERROR => "E_RECOVERABLE_ERROR",
			E_DEPRECATED => "E_DEPRECATED",
			E_USER_DEPRECATED => "E_USER_DEPRECATED",
		];
		$errno = isset($errorConversion[$errno]) ? $errorConversion[$errno] : $errno;
		if(($pos = strpos($errstr, "\n")) !== false){
			$errstr = substr($errstr, 0, $pos);
		}
		$oldFile = $errfile;
		$errfile = $this->cleanPath($errfile);

		$this->getLogger()->debug("An $errno error happened: \"$errstr\" in \"$errfile\" at line $errline");

		foreach(($trace = $this->getTrace($trace === null ? 3 : 0, $trace)) as $i => $line){
			$this->getLogger()->debug($line);
		}

		return true;
	}

	public function getTrace($start = 1, $trace = null){
		if($trace === null){
			if(function_exists("xdebug_get_function_stack")){
				$trace = array_reverse(xdebug_get_function_stack());
			}else{
				$e = new \Exception();
				$trace = $e->getTrace();
			}
		}

		$messages = [];
		$j = 0;
		for($i = (int) $start; isset($trace[$i]); ++$i, ++$j){
			$params = "";
			if(isset($trace[$i]["args"]) or isset($trace[$i]["params"])){
				if(isset($trace[$i]["args"])){
					$args = $trace[$i]["args"];
				}else{
					$args = $trace[$i]["params"];
				}
				foreach($args as $name => $value){
					$params .= (is_object($value) ? get_class($value) . " " . (method_exists($value, "__toString") ? $value->__toString() : "object") : gettype($value) . " " . @strval($value)) . ", ";
				}
			}
			$messages[] = "#$j " . (isset($trace[$i]["file"]) ? $this->cleanPath($trace[$i]["file"]) : "") . "(" . (isset($trace[$i]["line"]) ? $trace[$i]["line"] : "") . "): " . (isset($trace[$i]["class"]) ? $trace[$i]["class"] . (($trace[$i]["type"] === "dynamic" or $trace[$i]["type"] === "->") ? "->" : "::") : "") . $trace[$i]["function"] . "(" . substr($params, 0, -2) . ")";
		}

		return $messages;
	}

	public function cleanPath($path){
		return rtrim(str_replace(["\\", ".php", "phar://", rtrim(str_replace(["\\", "phar://"], ["/", ""], $this->mainPath), "/")], ["/", "", "", ""], $path), "/");
	}

    public function run() : void{
        //The thread is started with INHERIT_NONE, so it begins with an empty class table and an
        //empty include list. Composer's autoloader must therefore be loaded here, with require
        //and not require_once, exactly as PocketMine does in CommonThreadPartsTrait.
        require $this->composerAutoloaderPath;

	    gc_enable();
	    error_reporting(-1);
	    ini_set("display_errors", 1);
	    ini_set("display_startup_errors", 1);

	    set_error_handler([$this, "errorHandler"], E_ALL);
	    register_shutdown_function([$this, "shutdownHandler"]);


        try{
            $socket = new UDPServerSocket($this->getLogger(), $this->port, $this->interface);
            new SessionManager($this, $socket);
        }catch(\Throwable $e){
            //Last-resort net: set_error_handler() does NOT catch exceptions/Errors (e.g. ValueError from
            //str_split() on PHP 8+). Without this the thread would die silently and the interface would
            //report "RakLib Thread crashed", taking the network down. Log it instead.
            $this->getLogger()->logException($e);
        }
    }

}

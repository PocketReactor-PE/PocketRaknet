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

namespace pocketraknet\utils;

/**
 * Guards every call to the host logger.
 *
 * The RakLib thread starts with INHERIT_NONE: no inherited classes, functions or constants.
 * A host logger written for the main thread may therefore reference something the thread
 * cannot resolve, and the resulting Error is FATAL - it kills the thread and takes the whole
 * network interface down with it.
 *
 * That turns the safety net into the trap: SessionManager::tickProcessor() catches a malformed
 * packet, calls logException() to report it, and dies there. One hostile datagram is enough.
 *
 * So logging must never be able to throw. Anything the host logger raises is caught here and
 * rerouted to error_log(), which needs no class, no function and no constant.
 */
class SafeLogger implements Logger{

    /** @var Logger */
    private $inner;

    /** @var bool set once the inner logger has failed, so we stop paying for the attempt */
    private $broken = false;

    public function __construct(Logger $inner)
    {
        $this->inner = $inner;
    }

    public function emergency($message)
    {
        $this->relay("emergency", $message, "EMERGENCY");
    }

    public function critical($message)
    {
        $this->relay("critical", $message, "CRITICAL");
    }

    public function notice($message)
    {
        $this->relay("notice", $message, "NOTICE");
    }

    public function debug($message)
    {
        $this->relay("debug", $message, "DEBUG");
    }

    public function logException(\Throwable $e, $trace = null)
    {
        //Never let the host logger decide whether the thread lives. The fallback deliberately
        //rebuilds the message from the exception alone, without touching the host's helpers.
        if (!$this->broken) {
            try {
                $this->inner->logException($e, $trace);
                return;
            } catch (\Throwable $loggerFailure) {
                $this->broken = true;
                $this->fallback("RAKLIB", "logger failed while reporting an exception: " . $loggerFailure->getMessage());
            }
        }
        $this->fallback("EXCEPTION", get_class($e) . ': "' . $e->getMessage() . '" in "' . $e->getFile() . '" at line ' . $e->getLine());
        foreach (explode("\n", $e->getTraceAsString()) as $line) {
            $this->fallback("EXCEPTION", $line);
        }
    }

        private
        function relay($method, $message, $level)
        {
            if (!$this->broken) {
                try {
                    $this->inner->$method($message);
                    return;
                } catch (\Throwable $loggerFailure) {
                    //Latch: once the host logger has thrown, every later call would throw too.
                    $this->broken = true;
                    $this->fallback("RAKLIB", "logger failed: " . $loggerFailure->getMessage());
                }
            }
            $this->fallback($level, $message);
        }

        private
        function fallback($level, $message)
        {
            //error_log() is a language builtin: no autoloading, no constants, nothing that an
            //INHERIT_NONE thread could be missing. On the CLI it goes to stderr.
            @error_log("[RakLibServer thread/" . $level . "]: " . $message);
        }
}
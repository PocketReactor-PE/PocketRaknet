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
 * Logging contract required by RakLib.
 *
 * Only the methods RakLib actually calls are declared here; the host is free to
 * provide a richer logger. Signatures are deliberately untyped so that existing
 * PocketMine-SPL loggers can satisfy this interface without modification.
 */
interface Logger{

    /**
     * System is unusable.
     *
     * @param string $message
     */
    public function emergency($message);

    /**
     * Critical conditions.
     *
     * @param string $message
     */
    public function critical($message);

    /**
     * Normal but significant events.
     *
     * @param string $message
     */
    public function notice($message);

    /**
     * Detailed debug information.
     *
     * @param string $message
     */
    public function debug($message);

    /**
     * Logs a Throwable object.
     *
     * @param \Throwable $e
     * @param mixed      $trace
     */
    public function logException(\Throwable $e, $trace = null);
}

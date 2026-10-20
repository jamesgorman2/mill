package mill.client

import java.io.{ByteArrayOutputStream, InputStream, PrintStream}
import java.nio.charset.Charset

/**
 * Polls `file` every `intervalMsec` and prints every complete line appended to
 * it onto `stream`. A line is only printed once its terminating `\n` has been
 * written, so a line that is still being written never arrives in two parts.
 * [[flushNow]] reads whatever has been written so far from the calling thread,
 * and [[close]] stops the polling thread and performs that final read before
 * returning, so nothing written before `close()` is left behind.
 */
class FileToStreamTailer(file: os.Path, stream: PrintStream, intervalMsec: Int)
    extends Thread("FileToStreamTailerThread") with AutoCloseable {

  setDaemon(true)

  @volatile private var keepReading = true

  private val lock = new Object
  // All fields below are guarded by `lock`

  // if true, we won't read the whole file, but only new lines
  private var ignoreHead = true
  private var input: InputStream = null
  private val chunk = new Array[Byte](8192)
  // bytes of the current line, read but not yet terminated by '\n'
  private val partialLine = new ByteArrayOutputStream()

  override def run(): Unit = {
    while (keepReading) {
      readNewLines()
      try Thread.sleep(intervalMsec)
      catch { case _: InterruptedException => }
    }
  }

  override def interrupt(): Unit = {
    this.keepReading = false
    super.interrupt()
  }

  /**
   * Read and print everything written to the file so far, from the calling
   * thread. A trailing line that has not yet been terminated is printed as well,
   * so that nothing is left behind at the end of a command.
   */
  def flushNow(): Unit = lock.synchronized {
    readNewLines()
    if (partialLine.size() > 0) printLine()
  }

  override def close(): Unit = {
    interrupt()
    join()
    flushNow()
    lock.synchronized {
      if (input != null) {
        try input.close()
        catch { case _: java.io.IOException => }
        input = null
      }
    }
  }

  private def readNewLines(): Unit = lock.synchronized {
    if (input == null) openInput()
    if (input != null) {
      try {
        var n = input.read(chunk)
        while (n > 0) {
          var start = 0
          var i = 0
          while (i < n) {
            if (chunk(i) == '\n') {
              partialLine.write(chunk, start, i - start)
              printLine()
              start = i + 1
            }
            i += 1
          }
          partialLine.write(chunk, start, n - start)
          n = input.read(chunk)
        }
      } catch {
        case _: java.io.IOException => /* could not read or file vanished */
      }
      if (ignoreHead) {
        // we ignored once; an unterminated tail of the old content is old as well
        ignoreHead = false
        partialLine.reset()
      }
    }
  }

  // Check existence first to avoid allocating a NoSuchFileException on every
  // poll while the file is missing.
  private def openInput(): Unit = {
    if (os.exists(file)) {
      try input = os.read.inputStream(file)
      catch {
        case _: java.io.IOException =>
          // file vanished between the exists check and open
          ignoreHead = false
      }
    } else {
      // nothing to ignore if file is initially missing
      ignoreHead = false
    }
  }

  private def printLine(): Unit = {
    val bytes = partialLine.toByteArray
    partialLine.reset()
    val length =
      if (bytes.nonEmpty && bytes(bytes.length - 1) == '\r') bytes.length - 1 else bytes.length
    if (!ignoreHead) stream.println(new String(bytes, 0, length, Charset.defaultCharset()))
  }
}

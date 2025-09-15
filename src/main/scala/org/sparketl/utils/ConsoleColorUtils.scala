package org.sparketl.utils

object ConsoleColorUtils {
  private val RESET = "\u001b[0m"

  def red(str: String): String = s"\u001b[31m$str$RESET"
  def green(str: String): String = s"\u001b[32m$str$RESET"
  def yellow(str: String): String = s"\u001b[33m$str$RESET"
  def blue(str: String): String = s"\u001b[34m$str$RESET"
}

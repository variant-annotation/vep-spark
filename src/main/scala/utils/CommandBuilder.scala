package utils

import scala.collection.mutable

class CommandBuilder(var toolType: Tool.Type,
                     var executorDir: String = null,
                     var options: mutable.MutableList[ArgumentOption]) {

  def setExecutorDir(newExecutorDir: String) {
    new CommandBuilder(this.toolType, newExecutorDir, options)
  }

  def addOption(optionName: String, parameter: String = null, hasParameter: Boolean): CommandBuilder = {
    options += new ArgumentOption(optionName, parameter, hasParameter)
    new CommandBuilder(this.toolType, this.executorDir, options)
  }

  def build() = new Command(toolType, executorDir, options.toList)
}


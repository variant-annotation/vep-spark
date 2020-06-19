package utils

class Command(toolType: Tool.Type,
              executorDir: String,
              options: List[ArgumentOption]) {
  def generate: String = {
    val cmd: StringBuilder = new StringBuilder(executorDir)
    for (option <- options) {
      if (option.hasParameter)
        cmd.append(" ").append(option.optionName)
          .append(" ").append(option.parameter)
      else
        cmd.append(" ").append(option.optionName)
    }
    cmd.toString()
  }
}
package utils

object Tool {
  sealed abstract class Type
  object VEP extends Type
  object ANNOVAR extends Type
  object SNPeff extends Type
}
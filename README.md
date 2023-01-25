# City Traffic
Ce projet a été realiser par Hedi GASSARA et Omar GUERMAZI

On a un fichier filesToMap.csv ou on stocke les chemins des fichiers avec la class de Mapper correspondante.

Et pour executer il faut lancer la commande:
* HADOOP_CLASSPATH=/etc/hbase/conf:$(hbase mapredcp) yarn jar CityTraffic-1.0-SNAPSHOT.jar filesToMap.csv

on peut remplacer le fichier filesToMap.csv par un autre fichier de la meme format ou on peut ajouter d'autre ligne pour Mapper de nouveau fichiers.
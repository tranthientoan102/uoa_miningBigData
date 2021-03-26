<ol>
<h1><li>Run in stand alone mode</h1>
Execute StubDriver / WordCount with IDE support
<h1><li>Run in psuedo distributed mode</h1>

<ol>
<li> Run in cmd:<br>
a. compile java files

```shell
    hadoop com.sun.tools.javac.Main *.java
```
    
<code>javac</code> can be use but it would be more complicated since we need to specify Hadoop's path<br><br>
b. package into jar file
```shell    
    cd [path to java folder]
    jar cmf exercise2/MANIFEST.MF [wc.jar] exercise2/*.class
```
When need to switch between StubDriver and WordCound, we need to modify in MANIFEST.MF<br><br>
<li> Execute with <code>hadoop</code>

```shell
     hadoop jar wc.jar exercise2.WordCount ../resources/100-0.txt  ../resources/output4
```

</ol>

<h1><li>Run with maven</li></h1>

```shell
     mvn clean package 
     mvn test -P1
```



</ol>

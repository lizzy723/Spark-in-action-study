# Spark study 
참조 서적: Spark in Action (https://www.manning.com/books/spark-in-action) <br>
참조 github: https://github.com/spark-in-action/first-edition
<details close>
<summary>Original readme</summary>
  <h1>Spark in Action book repository</h1>
  <h4> Current edition: Manning Early Access Program (MEAP)</h4>

  The MEAP publishing date is **2015.04.04.**  
  Manning's book forum: [https://forums.manning.com/forums/spark-in-action](https://forums.manning.com/forums/spark-in-action)

  The repo contains book listings organized by chapter and programming language (if applicable):

  ```
  ch02
    ├── scala
    │    ├── ch02-listings.scala
    │    ├── scala-file.scala
    │    └── ...
    ├── java
    │    ├── Class1.java
    │    ├── Class2.java
    │    └── ...
    ├── python
    │    ├── ch02-listings.py
    │    ├── python-file.py
    │    └── ...
  ch03
    ├── scala
    │    ├── ch03-listings.scala
    │    ├── scala-file.scala
    │    └── ...
    ├── java
    │    ├── Class1.java
    │    ├── Class2.java
    │    └── ...
    ├── python
    │    ├── ch03-listings.py
    │    ├── python-file.py
    │    └── ...

  ```

  We tried to organize the listings so that you have minimum distractions while going through the book.  
  We hope you'll find the content useful and that you'll have fun reading the book and going through examples.  

  As part of Manning's "in action" series, it's a hands-on, tutorial-style book.  

  Thank you,  
  [Petar Zečević](http://twitter.com/p_zecevic) and [Marko Bonaći](http://twitter.com/markobonaci)
</details>


## 1장 First steps
<details close>
<summary><b>chapter 1</b> Introduction to apache spark</summary>
  <br>
  <blockquote>
  <details close>
  <summary>1.1 What is spark?</summary>
    <blockquote>
      
- **hadoop**: open source, distributed, Java computation framework consisting of the (1) **Hadoop Distributed File System(HDFS**) and (2) **MapReduce**, its execution engine. 
Hadoop solved the three main problems facing any distributed data-processing endeavor:
    - Parallelization— How to parallelize the computation (전체 연산을 잘게 나누어 동시에 처리하는 방법)
    - Distribution— How to distribute the data (데이터를 여러 노드로 분산하는 방법)
    - Fault-tolerance— How to handle component failure (분산 컴포넌트 장애에 대응하는 방법)

    → 하둡의 한계: 맵리듀스 잡의 결과를 다른 잡에서 사용하려면 먼저 이 결과를 HDFS에 저장해야 한다. 따라서 맵리듀스는 이전 잡의 결과가 다음 작업의 입력이 되는 반복 알고리즘에는 본질적으로 맞지 않다. 

- **Spark**와 hadoop의 차이: spark는 메모리에 아주 큰 데이터를 keep 하는 것을 가능하게 한다. 즉, 맵리듀스처럼 잡에 필요한 데이터를 디스크에서 매번 가져오는 대신(→ 하둡의 방법), 데이터를 메모리에 캐시로 저장하는 인-메모리 실행 모델에 있다. 이를 통해 스파크 프로그램은 맵리듀스보다 약 100배 더 빠른 속도로 같은 작업을 수행할 수 있다.
- 일부 애플리케이션은 스파크를 사용하기에 적합하지 않다. 스파크에서는 분산 아키텍처 때문에 처리 시간에 약간의 오버헤드가 필연적으로 발생한다. 대량의 데이터를 다룰 때는 오버헤드가 무시할 수 있는 수준이지만, 단일 머신에서도 충분히 처리할 수 있는 데이터셋을 다룰 때는 작은 데이터셋의 연산에 최적화된 다른 프레임워크(e.g. RDBMS)를 사용하는 것이 더 효율적이다. + 하지만 현대의 데이터 크기는 계속 증가하는 추세며, 언제가는 RDBMS와 스크립트 성능이 개선되는 속도를 추월할지도 모른다.
    </blockquote>
  </details>
    
  <details close>
  <summary>1.2 Spark components</summary>
    <blockquote><br>
스파크는 여러 특수한 목적에 맞게 설계된 다양한 컴포넌트로 구성된다.<br>
→ 스파크 core, 스파크 SQL, 스파크 Streaming, 스파크 GraphX, 스파크 MLlib <br><br>
<img width="629" alt="Screen Shot 2021-06-10 at 9 33 02 PM" src="https://user-images.githubusercontent.com/43725183/121525578-78936300-ca33-11eb-8dae-549e2a93f21a.png">

- **스파크 core**: 스파크 코어는 스파크 잡과 다른 스파크 컴포넌트에 필요한 기본 기능을 제공한다. RDD는 분산 데이터 컬렉션(즉, 데이터셋)을 추상화한 객체로 데이터셋에 적용할 수 있는 연산 및 변환 메서드를 함께 제공한다. RDD는 노드에 장애가 발생해도 데이터셋을 재구성할 수 있는 복원성을 갖추었다. broadcast variable(공유변수)와 accumulator(누적변수)를 사용해 컴퓨팅 노드 간에 정보를 공유할 수 있다. 이외에도 스파크 코어에는 네트워킹, 보안, 스케줄링 및 데이터 셔플링 등 기본 기능이 구현되어 있다.
- **스파크 SQL**
- **스파크 Streaming**
- **스파크 GraphX**
- **스파크 MLlib**
    </blockquote>
  </details>
    
  <details close>
  <summary>1.3 Spark program flow</summary>
    <blockquote><br>
예제를 통해 스파크 프로그램이 어떻게 실행되는지 알아보자.<br>
**[예제] 로그파일로부터 OutOfMemoryError 오류가 몇건 발생했는지 분석하기**

1. 노드 세 개로 구성된 하둡 클러스터에 300MB 크기의 로그 파일을 저장되어 있는 상황
→ HDFS는 자동으로 128MB크기의 chunck로 분할하고(하둡에서는 block이라는 용어를 사용한다.), 각 **블록(block)**을 클러스터의 여러 노드에 나누어 저장한다. 
→ 이 예제와 직접적인 관련은 없지만, HDFS는 노드에 저장한 각 블록을 다른 노드 두 개에 복제한다. (복제 계수가 기본 값은 3으로 설정된 경우)<br><img width="632" alt="Screen Shot 2021-06-10 at 9 55 04 PM" src="https://user-images.githubusercontent.com/43725183/121528532-85fe1c80-ca36-11eb-93e1-ba484fc4f839.png">

2. HDFS에 저장된 텍스트 파일을 메모리에 로드한다. 
→ 먼저 스파크 셸을 시작하고, 스파크 클러스터에 연결하기.
→ **데이터 지역성**(data locality, 즉 디스크에 저장된 파일이 모두 해당 노드 각각에서 RAM으로 로드되는 것/대량의 데이터를 네트워크로 전송해야하는 상황을 만들지 않기 위해!)을 최대한 달성하려고 로그 파일의 각 블록이 저장된 위치를 하둡에게 요청한 후, 모든 블록을 클러스터 노드의 RAM 메모리로 전송한다. 
→ RAM에 로딩된 블록을 **partition**이라고 한다. 이 파티션 집합이 바로 RDD가 참조하는 분산 컬렉션이다. RDD API를 사용해 RDD의 컬렉션을 필터링하고, 사용자 함수로 매핑하고, 누적 값 하나로 리듀스하고, 두 RDD를 서로 빼거나 교차하거나 결합하는 등 다양한 작업을 실행할 수 있다. 

    ```scala
    val lines = sc.textFile("hdfs://path/to/the/file")
    ```
      <img width="632" alt="Screen Shot 2021-06-10 at 9 55 21 PM" src="https://user-images.githubusercontent.com/43725183/121528576-90b8b180-ca36-11eb-8e1d-beca8097e67a.png">
      
3. 컬렉션이 OutOfMemoryError 문자열을 포함한 줄만 필터링
→ 필터링이 완료되면 RDD에는 분석에 필요한 데이터만 포함된다. 
→ 마지막으로 로그에 남은 줄 개수를 센다. 

    ```scala
    val oomLines = lines.filter(l => l.contains("OutOfMemoryError")).cache()
    val result = oomLines.count()
    ```
      <img width="634" alt="Screen Shot 2021-06-10 at 9 55 38 PM" src="https://user-images.githubusercontent.com/43725183/121528614-99a98300-ca36-11eb-85ff-f57f68c46ae6.png">
    </blockquote>
  </details>
    
  <details close>
  <summary>1.4 Spark ecosystem</summary>
    <blockquote><br>
      
      
* 하둡 생태계: 인터페이스 도구, 분석 도구, 클러스터 관리 도구, 인프라 도구로 구성되어 있다.
      
<br><img width="643" alt="Screen Shot 2021-06-10 at 9 45 04 PM" src="https://user-images.githubusercontent.com/43725183/121527201-1fc4ca00-ca35-11eb-957f-e701657ebda6.png">
- 하둡생태계의 일부 컴포넌트는 스파크 컴포넌트로 대체될 수 있다. <br>
e.g. Giraph → 스파크 GraphX, <br>
Mahout → 스파크 MLlib, <br>
Strom → 스파크 스트리밍, <br>
pig, sqoop → 스파크 코어, 스파크 SQL
- 하둡 생태계의 인프라 및 관리 도구들은 스파크로 대체할 수 없다. e.g. Oozie(→ Job scheduling), HBase, Zookeeper
- 스파크에 반드시 HDFS 스토리지를 사용할 필요는 없다. 스파크는 HDFS 외에도 **아마존 S3 버킷**이나 **일반 파일 시스템**에 저장된 데이터를 처리할 수 있다.
    </blockquote>
  </details>
    
  <details close>
  <summary>1.5 Setting up the spark-in-action VM</summary>
    <blockquote><br>

* vagrant와 virtualbox를 이용
    </blockquote>
  </details>
  </blockquote>
</details>


<details close>
<summary><b>chapter 2</b> Spark fundamentals</summary>
  <br><blockquote>
  <details close>
  <summary>2.1. Using the spark-in-action VM</summary>
    
```bash
$ vagrant up #가상 머신 시작
$ vagrant ssh -- -l spark #ssh 로그인

$ git clone https://github.com/spark-in-action/first-edition #github repository clone
```

cf. **하둡 사용하기**: `hadoop fs`로 시작한다. e.g. `hadoop fs -ls /user` (/user 폴더내 list 출력) → [http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html) 참고하기
  </details>
  <details close>
  <summary>2.2. Using Spark shell and writing your first Spark program</summary>

- license 파일을 읽어들이고, 이 파일에 포함된 BSD 문자열의 수를 세보자.

    ```python
    #license 파일을 읽기
    licLines = sc.textFile("/usr/local/spark/LICENSE")
    lineCnt = licLines.count()

    #BSD 문자열 필터링
    bsdLines = licLines.filter(lambda line: "BSD" in line)
    bsdLines.count()

    #BSD 문자열 출력
    bsdLines.foreach(lambda bLine: print(bLine))
    ```

- RDD의 개념: RDD는 스파크의 기본 추상화 객체로 다음과 같은 성질이 있다.
    - **불변성(immutable)**: 읽기 전용
    → RDD는 데이터를 조작할 수 있는 다양한 변환 연산자를 제공하지만, 변환 연산자는 항상 새로운 RDD를 생성한다.
    - **복원성(resilient)**: 장애 내성
    - **분산(distributed):** 노드 한개 이상에 저장된 데이터셋
- **RDD lineage**: 앞의 예제에서 LICENSE 파일을 로드해 `licLines` RDD를 생성했다. 그 다음 `licLines` RDD에 `filter` 함수를 적용해 새로운 `bsdLines` RDD를 생성했다. 이처럼 RDD에 적용된 변환 연산자와 그 적용 순서를 RDD lineage라고 한다.
  </details>
  <details close>
  <summary>2.3. Basic RDD actions and transformations</summary><br>
    
1. **transformation**: RDD를 조작해 새로운 RDD를 생성한다. e.g. filter, map
2. **actions**: 계산 결과를 반환하거나 RDD 요소에 특정 작업을 수행하려고 실제 계산을 시작하는 역할을 한다. e.g. count, foreach, collect, first

> 변환 연산자의 **지연 실행(lazy evaluation)**: 변환 연산자의 지연 실행은 행동 연산자를 호출하기 전까지는 변환 연산자의 계산을 실제로 실행하지 않는 것을 의미한다. RDD에 행동 연산자가 호출되면 스파크는 해당 RDD의 lineage를 살펴보고, 이를 바탕으로 실행해야 하는 연산 그래프를 작성해서 행동 연산자를 계산한다.  **결국 변환 연산자는 행동 연산자를 호출했을때 무슨 연산이 어떤 순서로 실행되어야 할지 알려주는 일종의 설계도라고 할 수 있다.** 

---

- **`map`**: RDD의 모든 요소에 임의의 함수를 적용할 수 있는 변환 연산자.

    ```python
    numbers = sc.parallelize(range(10, 51, 10))  #numbers = sc.makeRDD(range(10, 51, 10))도 가능.
    numbers.foreach(lambda x: print(x))
    numbersSquared = numbers.map(lambda num: num * num)
    numbersSquared.foreach(lambda x: print(x))
    ```

    `parallelize`는 sequential 객체를 받아 이 sequential 객체의 요소로 구성된 새로운 RDD를 만든다. 이 과정에서 sequential 객체의 요소는 여러 스파크의 executor로 **분산**된다. 
    (→ `print`결과를 보면 sequential 하지 않다. )

    ```python
    reversed = numbersSquared.map(lambda x: str(x)[::-1])
    reversed.foreach(lambda x: print(x))
    ```

- **`distinct`** and **`flatMap`**

    ```
    ##client-ids.log
    15,16,20,20
    77,80,94
    94,98,16,31
    31,15,20
    ```

    `flatMap`(주어진 모든 요소에 대해 연산을 진행하고, 반환한 배열의 중첩구조를 한 단계 제거하고 모든 요소를 단일 컬렉션으로 병합한다. )을 사용하면 각 요소별로 split을 진행한 결과를 하나의 array로 반환한다. 

    ```python
    lines = sc.textFile("Downloads/client-ids.log")

    ids = lines.flatMap(lambda x: x.split(","))
    ids.collect()   #리스트를 반환한다. 
    ids.first()

    intIds = ids.map(lambda x: int(x))
    intIds.collect()

    uniqueIds = intIds.distinct()
    uniqueIds.collect()
    finalCount  = uniqueIds.count()
    finalCount  #8
    ```

- **`sample`**, **`take`**, **`takeSample`** 연산으로 RDD 일부 요소 가져오기: `sample`은 변환연산자(transformation)이지만, `take`와 `takeSample`은 행동연산자(Actions)이다.

    ```python
    ##비복원 샘플링
    s = uniqueIds.sample(False, 0.3)  #withReplacement=False
    s.count()
    s.collect()

    ##복원 샘플링
    swr = uniqueIds.sample(True, 0.5)
    swr.count
    swr.collect()

    #확률값대신 정확한 개수로 RDD 요소 샘플링하기
    taken = uniqueIds.takeSample(False, 5)  #list 반환
    uniqueIds.take(3)
    ```

    `take`는 지정된 개수의 요소를 모을 때까지 RDD의 파티션을 하나씩 처리해 결과를 반환한다. → 결국 연산이 전혀 분산되지 않는다는 의미이다. 여러 파티션의 요소를 빠르게 가져오고 싶다면 드라이버의 메모리를 넘지 않도록 요소 개수를 적당히 줄이고 `collect` 연산자를 사용하자.
  </details>
  <details close>
  <summary>2.4. Double RDD functions</summary>
    
* Double(cf. float) 객체만 사용해서 RDD를 구성하면 implicit conversion을 통해 몇 가지 추가 함수를 사용할 수 있다.

    ```python
    intIds.mean()
    intIds.sum()

    intIds.stats()
    intIds.variance()
    intIds.stdev()

    intIds.histogram([1.0, 50.0, 100.0])  #1과 50사이에 몇개, 50과 100사이에 몇개가 있는지 반환
    intIds.histogram(3)  #3개의 경계로 나눴을때 (1) 경계값과, (2) 구간에 존재하는 sample의 수를 반환한다. 
    ```

* 근사 합계 및 평균계산: 지정된 제한시간 동안 근사합계 또는 근사 평균을 계산한다. 제한시간을 인자로 넣어준다.

    ```python
    intIds.sumApprox(1)
    intIds.meanApprox(1)
    ```
  </details></blockquote>
</details>
  
<details close>
<summary><b>chapter 3</b> Writing spark application</summary>
  <br><blockquote>
  <details close>
  <summary>3.1. Generating a new Spark project in Eclipse</summary

<br>Eclipse 대신 pycharm 사용하기
  </details>
  <details close>
  <summary>3.2. Developing the application</summary><br>
    
**[예제] 한 게임 회사의 모든 직원 명단과 각 직원이 수행한 푸시 횟수를 담은 일일 리포트를 개발해보자.**

1. 데이터셋 준비

    ```bash
    $ mkdir -p $HOME/sia/github-archive
    $ cd $HOME/sia/github-archive
    $ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz

    $ gunzip *

    $ head -1 2015-03-01-0.json| jq '.'
    ```
    <img width="400" alt="Screen Shot 2021-06-16 at 7 32 50 PM" src="https://user-images.githubusercontent.com/43725183/122204059-a5cf8d80-ced9-11eb-9334-c0b2309e6910.png">

    > actor.login이 "treydock"인 누군가가 2015년 3월 1일 자정(created_at)에 "development "라는 저장소 브랜치를 생성한 기록이다. (깃허브 API 문서 참조: [https://developer.github.com/v3/activity/events/types/](https://developer.github.com/v3/activity/events/types/))

2. JSON 로드: `read.json()` 은 한 줄당 JSON 객체 하나가 저장된 파일을 로드한다. DataFrame 객체를 반환한다. 

    ```python
    from pyspark.sql import SQLContext
    import sys

    sqlContext = SQLContext(sc)
    ghLog = sqlContext.read.json("2015-03-01-0.json")
    ghLog.printSchema()
    ghLog.show(5)

    ghLog.count() #17786
    pushes = ghLog.filter("type = 'PushEvent'")
    pushes.count()  #8793
    ```
    <img width="1000" alt="Screen Shot 2021-06-16 at 7 35 14 PM" src="https://user-images.githubusercontent.com/43725183/122204352-fc3ccc00-ced9-11eb-8404-29866a84399e.png">

3. 데이터 집계: `count`외에도 `min`, `max`, `avg`, `sum` 등의 집계 함수를 제공한다. 

    ```python
    grouped = pushes.groupBy("actor.login").count()
    ordered = grouped.orderBy(grouped['count'], ascending=False)
    ```

4. 분석대상 제외 & 공유변수

    : `broadcast`를 이용해서 **공유변수**를 만들자. 공유변수를 설정하지 않고 이대로 예제 프로그램을 실행하면 스파크는 `employees`를 대략 200회(필터링 작업을 수행할 태스크 개수) 가까이 반복적으로 네트워크에 전송하게 될 것이다. 

    반면 공유변수는 클러스터의 각 노드에 정확히 한 번만 전송한다. 또 공유변수는 클러스터 노드의 메모리에 자동으로 캐시되므로 프로그램 실행 중 바로 접근할 수 있다. 

    ```python
    employees = [line.rstrip('\n') for line in open(sys.argv[2])]
    bcEmployees = sc.broadcast(employees)

    def isEmp(user):
    	return user in bcEmployees.value

    #filter에 사용하기 위해서 user-defined function 등록
    from pyspark.sql.types import BooleanType
    sqlContext.udf.register("SetContainsUdf", isEmp, returnType=BooleanType())
    filtered = ordered.filter("SetContainsUdf(login)")
    ```

5. 애플리케이션 실행

    ```bash
    $ python main.py \
    	../github-archive/2015-03-01-0.json \   #	../github-archive/*.json \
    	../ch03/ghEmployees.txt
    ```
    <img width="200" alt="Screen Shot 2021-06-16 at 7 37 01 PM" src="https://user-images.githubusercontent.com/43725183/122204609-46be4880-ceda-11eb-8628-b179a7dedcfa.png">

    ```python
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext
    import sys

    if __name__ == "__main__":
    	sc = SparkContext(conf=SparkConf())
    	sqlContext = SQLContext(sc)
    	ghLog = sqlContext.read.json(sys.argv[1])

    	pushes = ghLog.filter("type = 'PushEvent'")
    	grouped = pushes.groupBy("actor.login").count()
    	ordered = grouped.orderBy(grouped['count'], ascending=False)

    	# Broadcast the employees set	
    	employees = [line.rstrip('\n') for line in open(sys.argv[2])]

    	bcEmployees = sc.broadcast(employees)

    	def isEmp(user):
    		return user in bcEmployees.value

    	from pyspark.sql.types import BooleanType
    	sqlContext.udf.register("SetContainsUdf", isEmp, returnType=BooleanType())
    	filtered = ordered.filter("SetContainsUdf(login)")
    	filtered.show()

    	filtered.write.format(sys.argv[4]).save(sys.argv[3])
       #파일 형식에는 JSON, Parquet, JDBC등이 가능하다. 
       #세번째 인수는 저장 경로를 적어준다. 
    ```

  </details>
  <details close>
  <summary>3.3. Submitting the application</summary><br>

스파크 프로그램을 운영 환경에서 원활하게 실행할 수 있도록 프로그램에 JAR 파일을 추가하는 방법은 (1) `spark-submit` 스크립트의 `—jars` 매개변수에 프로그램에 필요한 JAR 파일을 모두 나열해 실행자로 전송하거나, (2) 모든 의존 라이브러리를 포함하는 `underjar`를 빌드하는 방법이다. 

- uderjar 빌드: 일단은 생략
- `spark-submit`: 스파크 애플리케이션을 제출하는 일종의 헬퍼 스크립트로 애플리케이션을 스파크 클러스터에서 실행하는데 사용한다.

    ```bash
    ./bin/spark-submit \
      --class <main-class> \   #python에서는 사용 x
      --master <master-url> \
      --deploy-mode <deploy-mode> \
      --conf <key>=<value> \
      ... # other options
      <application-jar> \
      [application-arguments]
    ```

    결과 폴더에 있는 `SUCCESS` 파일은 작업을 성공적으로 완료했음을 의미한다. 각 `crc` 파일은 CRC(Cyclic Redundancy Check)코드를 계산해 각 데이터 파일의 유효성을 검사하는데 사용한다. `._SUCCESS.crc` 파일은 모든 CRC파일을 검사한 결과가 성공적이라는 것을 의미한다.  

    ```bash
    $ spark-submit \
    	main.py \
    	"../github-archive/2015-03-01-0.json" \ 
    	"../ch03/ghEmployees.txt" \
    	"emp-gh-push-output" "json"
    ```
        
    <img width="1000" alt="Screen Shot 2021-06-17 at 12 08 30 PM" src="https://user-images.githubusercontent.com/43725183/122324988-bc6ef680-cf64-11eb-946b-4ede59f690a4.png">

  </details></blockquote>
</details>
  
<details close>
<summary><b>chapter 4</b> The spark api in depth</summary>
  <br><blockquote>
  <details close>
  <summary>4.1. Working with pair RDDs</summary><br>
    
키-값 쌍은 전통적으로 연관배열(associative array)이라는 자료구조를 사용해 표현한다. 이 자료구조를 파이썬에서는 dictionary라고 하며, 스칼라와 자바에서는 map이라고 한다. 스파크에서는 키-값 쌍으로 구성된 RDD를 **pair RDD**라고 한다. 스파크의 데이터를 반드시 키-값 쌍 형태로 구성할 필요는 없지만, 실무에서는 Pair RDD를 사용하는 것이 바람직한 경우가 많다. 

- **Pair RDD 생성**: RDD의 keyBy 변환 연산자는 RDD 요소로 키를 생성하는 f 함수를 받고, 각 요소를 (f(요소), 요소)( = **(키, 요소)**) 쌍의 튜플로 매핑한다. 물론 데이터를 2-element 튜플로 직접 변환할 수도 있다. 어떤 방법이든 2-element 튜플로 RDD를 구성하면 **Pair RDD 함수**가 RDD에 자동으로 추가된다.

    ```python
    tranFile = sc.textFile("ch04/ch04_data_transactions.txt")
    tranData = tranFile.map(lambda line: line.split("#"))
    #예제 파일의 각 줄에는 구매 날짜, 시간, 고객 ID, 상품 ID, 구매수량, 구매 금액이 '#'로 구분되어있다. 
    ```
    <img width="400" alt="Screen Shot 2021-06-18 at 4 42 12 PM" src="https://user-images.githubusercontent.com/43725183/122525386-24503a80-d054-11eb-996d-d9a75f962093.png">

    ```python
    #키, 요소 페어 생성
    transByCust = tranData.map(lambda t: (int(t[2]), t))

    transByCust.keys().distinct().count() #100
    transByCust.count() #1000
    #map 함수로 키와 값을 만들어보자. 고객이 총 100명 있다.
    ```

- **Pair RDD 함수**
    - 키별 개수 세기(`CountByKey`): 각 키의 출현 횟수를 담은 Map 객체를 반환한다. ID가 53인 고객이 19번으로 가장 많이 구매했다.

        ```python
        transByCust.countByKey()
        cid, purch = sorted(transByCust.countByKey().items(), key= lambda x:-x[1])[0] ##(53, 19)

        #사은품 리스트 추가(가격 = 0)
        complTrans = [["2015-03-30", "11:59 PM", "53", "4", "1", "0.00"]]
        ```

    - 단일 키로 값 찾기: `transByCust.lookup(53)` → 53번 고객의 모든 구매 기록을 가져올 수 있다. lookup은 결과 값을 드라이버로 전송하므로 이를 메모리에 적재할 수 있는지 먼저 확인해야 한다.
        
        <img width="300" alt="Screen Shot 2021-06-18 at 4 43 00 PM" src="https://user-images.githubusercontent.com/43725183/122525491-3f22af00-d054-11eb-898a-df7f06895a28.png">

 

    - `mapValues` 변환 연산자로 Pair RDD 값 바꾸기: 키를 그대로 두고 값만 변경할 수 있다. 25번 상품을 2번 이상 구매한 경우 5% 할인해주자.

        ```python
        def applyDiscount(tran):
            if(int(tran[3])==25 and float(tran[4])>1):
                tran[5] = str(float(tran[5])*0.95)
            return tran

        transByCust = transByCust.mapValues(lambda t: applyDiscount(t))
        ```

    - `flapMapValues` 변환 연산자로 키에 값 추가: 81번 다섯 개 이상 구매한 고객에게 사은품으로 70번을 보내야한다. 즉, 해당 고객이 70번 상품을 구매한 것 처럼 `transByCust`에 추가해야한다.

        ```python
        def addToothbrush(tran):
            if(int(tran[3]) == 81 and int(tran[4])>4):
                from copy import copy
                cloned = copy(tran)
                cloned[5] = "0.00"
                cloned[3] = "70"
                cloned[4] = "1"
                return [tran, cloned]
            else:
                return [tran]

        transByCust = transByCust.flatMapValues(lambda t: addToothbrush(t))
        transByCust.count()  #1006 -> 6명이 81번 상품을 5개 이상 구매했다.
        ```

    - `reduceByKey` 또는 `foldByKey` 변환 연산자로 키의 모든 값 병합하기: reduceByKey는 각 키의 모든 값을 동일한 타입의 단일 값으로 변환한다. 이 연산자에는 두 값을 하나로 병합하는 Merge 함수를 전달해야하며, reduceByKey는 각 키별로 값 하나만 남을 때까지 merge 함수를 계속 호출한다. 따라서 merge 함수는 결합법칙을 만족해야한다.

        ```python
        amounts = transByCust.mapValues(lambda t: float(t[5])) #각 키별로 값만 뽑기 
        totals = amounts.foldByKey(0, lambda p1, p2: p1 + p2).collect()
        sorted(totals, key = lambda x:x[1])[-1] #(76, 100049.0)

        #사은품 리스트 추가(가격 = 0)
        complTrans += [["2015-03-30", "11:59 PM", "76", "63", "1", "0.00"]]
        #76번 고객이 10만 49달러로 가장 많이 썼다. 
        ```

        foldByKey는 reduceByKey와 기능은 같지만, merge 함수의 인자 목록 바로 앞에 zeroValue 인자를 담은 또 다른 인자 목록을 추가로 전달해야 한다는 점은 다르다. zeroValue는 반드시 항등원이어야한다. (e.g. 덧셈에서는 0, 곱셈에서는 1)

    - `union`으로 array 추가하기 + 결과 파일 저장하기

        ```python
        transByCust = transByCust.union(sc.parallelize(complTrans).map(lambda t: (int(t[2]), t)))
        transByCust.map(lambda t: "#".join(t[1])).saveAsTextFile("ch04/ch04output-transByCust")
        ```

    - `aggregateByKey`로 키의 모든 값 그루핑: aggregateByKey는 (1) **zeroValue**와 (2) 임의의 V 타입을 가진 값을 또 다른 U 타입으로 변환하는 **변환 함수(Transform)**, (3)변환함수가 변환한 값을 두 개씩 하나로 병합하는 **병합함수(Merge)**가 필요하다.

        ```python
        prods = transByCust.aggregateByKey([], 
        		lambda prods, tran: prods + [tran[3]],    #변환함수 -> 각 파티션 별로 요소를 병합
            lambda prods1, prods2: prods1 + prods2)   #병합함수 -> 최종 결과를 병합
        prods.collect()
        #prods에 빈 리스트가 전달되었다.
        ```
        <img width="1000" alt="Screen Shot 2021-06-18 at 4 43 31 PM" src="https://user-images.githubusercontent.com/43725183/122525565-52357f00-d054-11eb-8eb7-b7f6cba80cd0.png">

  </details>
  <details close>
  <summary>4.2. Understanding data partitioning and reducing data shuffling</summary>
    
* **데이터 파티셔닝(data partitioning)** : 데이터를 여러 클러스터 노드로 분할하는 메커니즘을 의미한다. 이 장에서는 일단 스파크 클러스터를 '병렬 연산이 가능하고 네트워크로 연결된 머신(즉, 노드)의 집합'정도로 생각하자.
    - **파티션(partition)**: 과거에는 파티션 대신 스플릿(split)용어를 사용했다. RDD의 파티션은 RDD 데이터의 일부(조각 또는 슬라이스)를 의미한다. 예를 들어 로컬 파일 시스템에 저장된 텍스트 파일을 스파크에 로드하면, 스파크는 파일 내용을 여러 파티션으로 분할해 클러스터 노드에 고르게 분산 저장한다. 여러 파티션을 노드 하나에 저장할 수도 있다. 이렇게 분산된 파티션이 모여서 RDD 하나를 형성한다.
    - 파티션의 개수: 해당 RDD에 변환 연산을 실행할 "태스크 개수"와 직결되므로 파티션의 개수는 매우 중요하다. 태스크 개수가 필요 이하로 적으면 클러스터를 충분히 활용할 수 없다. 게다가 각 태스크가 처리할 데이터 분량이 실행자의 메모리 리소스를 초과해 메모리 문제가 발생할 수 있다. 따라서 클러스터의 코어 개수보다 서너 배 더 많은 파티션을 사용하는 것이 좋다.
    - **데이터 partitioner**: RDD의 데이터 파티셔닝은 RDD의 각 요소에 파티션 번호를 할당하는 partitioner 객체가 수행한다. partitioner는 HashPartitioner, RangePartitioner, 또는 사용자 정의 Partitioner(Pair RDD의 경우)로 구현할 수 있다.
        - **HashPartitioner**: 스파크의 기본 Partitioner. HashPartitioner는 각 요소의 자바 해시코드를 단순한 mod 공식(partitionIndex = hashCode % numberOfPartitions)에 대입해 파티션 번호를 계산한다. 각 요소의 파티션 번호를 거의 무작위로 결정하기 때문에 모든 파티션을 정확하게 같은 크기로 분할할 가능성이 낮다. 하지만 대규모 데이터셋을 상대적으로 적은 수의 파티션으로 나누면 대체로 데이터를 고르게 분산시킬 수 있다. 
        (파티션의 기본 개수는 spark.default.parallelism 환경 매개변수 값으로 결정된다)
        - **RangePartitioner**: 정렬된 RDD의 데이터를 거의 같은 범위 간격으로 분할할 수 있다.
        - **Pair RDD의 사용자 정의 Partitioner**: 파티션의 데이터를 특정 기준에 따라 정확하게 배치해야 할 경우 사용자 정의 partitioner로 pair RDD를 분할할 수 있다. 
        (cf. in Python there is no version of aggregateByKey with a custom partitioner)
    - RDD 파티션 변경
        - `partitionBy`: PairRDD에서만 사용가능하고, 또 파티셔닝에 사용할 Partitioner 객체만 인자로 전달할 수 있다
        - `coalesce`와 `repartition`: coalesce는 파티션의 수를 줄이거나 늘리는데 사용한다. 파티션 개수를 늘리려면 shuffle 인자를 true로 설정해야한다. 반면 파티션 수를 줄일 때는 이 인자를 false로 설정할 수 있다. 이때는 새로운 파티션 개수와 동일한 개수의 부모 RDD 파티션을 선정하고 나머지 파티션의 요소를 나누어 선정한 파티션과 병합(coalesce)하는 방식으로 파티션 개수를 줄인다. 즉, 셔플링을 수행하지 않는 대신 데이터 이동을 최소화하려고 부모 RDD의 기존 파티션을 최대한 보존한다. 
        cf. repartition 변환 연산자는 단순히 shuffle을 true로 설정해 coalesce를 호출한 결과를 반환한다.
        - `repartitionAndSortWithinPartition`: 정렬 가능한 RDD에서만 사용할 수 있다. 새로운 Partitioner 객체를 받아 각 파티션 내에서 요소를 정렬한다. 이 연산자는 셔플링 단계에서 정렬 작업을 함께 수행하기 때문에 repartition을 호출한 후 직접 정렬하는 것보다 성능이 더 낫다.
- **데이터 셔플링(data shuffling)**: **파티션 간의 물리적인 데이터 이동을 의미한다.** 셔플링은 새로운 RDD의 파티션을 만들려고 여러 파티션의 데이터를 합칠 때 발생한다. 예를 들어 키를 기준으로 요소를 그루핑하려면 스파크는 RDD의 파티션을 모두 살펴보고 키가 같은 요소를 전부 찾은 후, 이를 물리적으로 묶어서 새로운 파티션을 구성하는 과정을 수행해야한다.

    ```python
    prods = transByCust.aggregateByKey([], 
    		lambda prods, tran: prods + [tran[3]],    #변환함수(Transform) -> 각 파티션 별로 요소를 병합
        lambda prods1, prods2: prods1 + prods2)   #병합함수(Merge) -> 최종 결과를 병합
    prods.collect()
    #4.1절의 aggregateByKey 예시를 다시 보자. 
    ```
    <img width="600" alt="Screen Shot 2021-06-18 at 4 45 57 PM" src="https://user-images.githubusercontent.com/43725183/122525886-a93b5400-d054-11eb-9dcd-ea556f230a4d.png">


    - 변환함수(Transform)은 각 파티션 별로 각 키의 값을 모아서 리스트를 구성한다.(**map task**) →  스파크는 이 리스트들을 각 노드의 중간 파일(interm files)에 기록한다 → 병합함수(merge)를 호출해 여러 파티션에 저장된 리스트들을 각 키별 단일 리스트로 병합한다.(**reduce task**) → 기본 partitioner(hash partitioner)를 적용해 각 키를 적절한 파티션에 할당한다.
    - 셔플링 바로 전에 수행한 태스크를 **맵(map) 태스크**라고 하며, 바로 다음에 수행한 태스크를 **리듀스(reduce)** 태스크라고 한다. 맵 태스크의 결과는 중간 파일에 기록하며(주로 운영체제의 파일 시스템 캐시에만 저장), 이후 리듀스 태스크가 이 파일을 읽어들인다. 중간 파일을 디스크에 기록하는 작업도 부담이지만, 결국 셔플링할 데이터를 네트워크로 전송해야 하기 때문에 스파크 잡의 셔플링 횟수를 최소한으로 줄이도록 노력해야 한다.
    - 셔플링 발생 조건
        1. partitioner를 명시적으로 변경하는 경우
            - 파티션 개수가 다른 HashPatitioner를 변환 연산자에 사용하거나, (`rdd.aggregateByKey(zeroValue, seqFunc, comboFunc, 100).collect()`)
            - 사용자 정의 Partitioner를 사용하면

            → 셔플링이 발생한다. 따라서 가급적이면 기본 partitioner를 사용해 의도하지 않은 셔플링은 최대한 피하는 것이 성능 면에서 가장 안전한 방법이다. 

        2. partitioner를 제거하는 경우

            변환 연산자에 partitioner를 명시적으로 지정하지 않았는데도 간혹 셔플링이 발생할 때가 있다. 대표적으로 map과 flatMap은 RDD의 Partitioner를 제거한다. 이 연산자 자체로는 셔플링이 발생하지 않지만, 연산자의 결과 RDD에 다른 변환 연산자를 사용하면 기본 Partitioner를 사용했더라도 여전히 셔플링이 발생한다.  → Pair RDD의 키를 변경하지 않는다면 map, flatMap 대신 mapValues, flatMapValues를 사용해 Partitioner를 보존하는 것이 좋다. 

            ```python
            rdd = sc.parallelize(range(10000))
            rdd.map(lambda x: (x, x*x)).map(lambda (x, y): (y, x)).collect() #셔플링 발생 x
            rdd.map(lambda x: (x, x*x)).reduceByKey(lambda v1, v2: v1+v2).collect() #셔플링 발생함
            ```

    - 셔플링을 수행하면 executor는 다른 executor(=실행자)의 파일을 읽어 들여야한다. 하지만 셔플링 도중 일부 실행자에 장애가 발생하면 해당 실행자가 처리한 데이터를 더 이상 가져올 수 없어서 데이터 흐름이 중단된다. → **외부 셔플링 서비스(external shuffling service)**는 실행자가 중간 셔플 파일을 읽을 수 있는 단일 지점을 제공해 셔플링의 데이터 교환 과정을 최적화할 수 있다. (`spark.shuffle.service.enabled=true`로 설정하기)
    - 셔플링 관련 매개변수
        - 셔플링 알고리즘 설정: `spark.shuffle.manager`의 값을 hash(해시 기반 셔플링), sort(정렬 기반 셔플링) → 정렬 기반 셔플링은 파일을 더 적게 생성하고 메모리를 더욱 효율적으로 사용할 수 있어 default 값이다.
        - 중간 파일의 통합 여부: `spark.shuffle.consolidateFiles` → ext4나 XFS 파일 시스템을 사용한다면 이 값을 true로 변경(default = false)로 하는 것이 좋다.
        - 셔플링에 쓸 메모리 리소스의 제한 여부: `spark.shuffle.spill` → 메모리를 제한하면 스파크는 제한 임계치를 초과한 데이터를 디스크로 내보낸다. 메모리 임계치를 너무 높게 설정하면 메모리 부족 예외(out-of-memory exception)이 발생할 수 있다. 반대로 너무 낮게 설정하면 데이터를 자주 내보내므로 균형을 잘 맞추는 것이 중요하다.
        - `spark.shuffle.compress`: 중간 파일의 압축 여부를 지정할 수 있다.
        - `spark.shuffle.spill.batchSize`: 데이터를 디스크로 내보낼 때 일괄로 직렬화 또는 역직렬화할 객체 개수를 지정한다.
        - `spark.shuffle.service.port`: 외부 셔플링 서비스를 활성화할 경우 서비스 서버가 사용할 포트 번호를 지정한다.
- 파티션 단위로 데이터 매핑: RDD의 각 파티션에 개별적으로 매핑 함수를 적용할수도 있다. 이 메서드를 잘 활용하면 각 파티션 내에서만 데이터가 매핑되도록 기존 변환 연산자를 최적화해 셔플링을 억제할 수 있다.
    - `mapPartitions`와 `mapPartitionsWithIndex`: mapPartitions는 각 파티션의 모든 요소를 반복문으로 처리하고 새로운 RDD 파티션을 생성한다. mapPartitionWithIndex는 매핑 함수에 파티션 번호가 함께 전달된다.
    - `glom`: 각 파티션의 모든 요소를 배열 하나로 모으고, 이 배열들을 요소로 포함하는 새로운 RDD를 반환한다. 따라서 새로운 RDD에 포함된 요소 개수는 이 RDD의 파티션 개수와 동일하다. glom 연산자는 기존의 Partitioner를 제거한다.

        ```python
        import random
        l = [random.randrange(100) for x in range(500)]
        rdd = sc.parallelize(l, 30).glom() #30개의 파티션으로 나눠져 있는 것을 모음. 
        rdd.collect()
        rdd.count() #30
        ```
  </details>
  <details close>
  <summary>4.3. Joining, sorting, and grouping data</summary>
    
* **데이터 조인(Join)** : 스파크에서는 RDBMS의 고전적인 join 연산자뿐만 아니라, zip, cartesian, intersection 등 다양한 변환 연산자를 사용해 여러 RDD 내용을 합칠 수 있다.

    <img width="621" alt="Screen Shot 2021-06-20 at 9 57 57 PM" src="https://user-images.githubusercontent.com/43725183/122675025-940c2400-d212-11eb-9db3-769ed31b6e25.png">

    RDD의 key를 product ID로 변경

    - RDBMS와 유사한 Join 연산자: `join`(RDBMS의 inner join과 동일), `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin` → 이 연산자들은 PairRDD에서만 사용할 수 있다. Partitioner 객체나 파티션 개수를 전달할 수 있다.

        ```python
        totalsByProd = transByProd.mapValues(lambda t: float(t[5])).reduceByKey(lambda tot1, tot2: tot1 + tot2)

        products = sc.textFile("first-edition/ch04/ch04_data_products.txt").map(lambda line: line.split("#")).map(lambda p: (int(p[0]), p))
        totalsAndProds = totalsByProd.join(products)
        totalsAndProds.first()
        #(68, (62133.899999999994, ['68', 'Niacin', '6295.48', '1']))
        #68은 key(productID), 그리고 value의 첫번째는 totalByProd의 값, 두번째 리스트는 products 정보.
        ```

        ```python
        totalsWithMissingProds = products.leftOuterJoin(totalsByProd)
        missingProds = totalsWithMissingProds.filter(lambda x: x[1][1] is None).map(lambda x: x[1][0])
        missingProds.foreach(lambda p: print(", ".join(p)))
        #3, Cute baby doll, battery, 1808.79, 2
        #43, Tomb Raider PC, 2718.14, 1
        #63, Pajamas, 8131.85, 3
        #20, LEGO Elves, 4589.79, 4
        #어제 판매하지 않은 상품 리스트 출력. 
        ```

    - `subtract`이나 `subtractByKey` 변환 연산자로 공통 값 제거: `substract`은 첫번째 RDD에서 두번째 RDD의 요소를 제거한 여집합을 반환한다. 이는 일반 RDD에서도 사용할 수 있으며, 각 요소의 키나 값만 비교하는 것이 아니라 요소 전체를 비교해 제거여부를 판단한다. 
    반면 `substracByKey`는 PairRDD에서만 사용할 수 있으며 키와 값을 모두 보는 것이 아니라 키만 본다.

        ```python
        missingProds = products.subtractByKey(totalsByProd)
        missingProds.foreach(lambda p: print(", ".join(p[1])))
        #leftOuterJoin후 None 값인 record만 뽑아내는 과정대신 substractByKey를 사용하면 된다. 
        ```

    - `cogroup` 변환 연산자로 RDD 조인: `cogroup`은 여러 RDD 값을 각각 키로 그루핑한 후 키를 기준으로 조인한다. `cogroup`은 RDD를 최대 세개까지 조인할 수 있는데, 단 `cogroup`을 호출한 RDD와 `cogroup`에 전달된 RDD는 모두 동일한 타입의 키를 가져야 한다.

        ```python
        prodTotCogroup = totalsByProd.cogroup(products)
        prodTotCogroup.first()
        #(68, (<pyspark.resultiterable.ResultIterable object at 0x107d3ab90>, <pyspark.resultiterable.ResultIterable object at 0x107da1fd0>))

        prodTotCogroup.filter(lambda x: len(x[1][0].data) == 0).foreach(lambda x: print(", ".join(x[1][1].data[0])))
        #total 가격과 product 정보의 outer join. 두 RDD 중 한쪽에만 등장한 키의 경우 다른 쪽 RDD iterator는 비어 있다. 
        ```

    - `intersection` 변환 연산자 사용: `intersection`은 교집합을 찾는다.

        ```python
        totalsByProd.map(lambda t: t[0]).intersection(products.map(lambda p: p[0]))
        ```

    - `cartesian` 변환 연산자로 RDD 두개 결합: 두 RDD의 데카르트 곱을 계산한다(요소의 모든 combination return). 따라서 이를 이용해 두 RDD 요소들을 서로 비교하는데도 사용할 수 있다.

        ```python
        rdd1 = sc.parallelize([7,8,9])
        rdd2 = sc.parallelize([1,2,3])
        rdd1.cartesian(rdd2).collect()
        #[(7, 1), (7, 2), (7, 3), (8, 1), (8, 2), (8, 3), (9, 1), (9, 2), (9, 3)]
        rdd1.cartesian(rdd2).filter(lambda el: el[0] % el[1] == 0).collect()
        #rdd2숫자로 rdd1 숫자를 나머지 없이 나눌 수 있는 조합 찾기
        [(7, 1), (8, 1), (8, 2), (9, 1), (9, 3)]
        ```

    - `zip` 변환 연산자로 RDD 조인: 모든 RDD에 사용가능. 첫번째 쌍은 각 RDD의 첫번째 요소를 조합한 것이며, 두번째 쌍은 두 번째 요소를 조합하고, 세 번째 쌍은 세 번째 요소를 조합하는 식으로 끝까지 이어진다. → 두 RDD의 파티션 개수가 다르거나 두 RDD의 모든 파티션이 서로 동일한 개수의 요소를 포함하지 않으면 오류를 발생한다.

        ```python
        rdd1 = sc.parallelize([1,2,3])
        rdd2 = sc.parallelize(["n4","n5","n6"])
        rdd1.zip(rdd2).collect()
        #[(1, 'n4'), (2, 'n5'), (3, 'n6')]
        ```

    - `zipPartitions` 변환 연산자로 RDD 조인: 모든 RDD에 사용가능. RDD는 서로 파티션 개수가 동일해야하지만, 모든 파티션이 서로 동일한 개수의 요소를 가져야 한다는 조건을 꼭 만족하지 않아도 된다. → 아직 python에서는 사용할 수 없다.
- **데이터 정렬(sorting)**
    - `repartitionAndSortWithinPartition`(4.2 절 참조), `sortByKey`, `sortBy`을 사용해서 정렬할 수 있다.

        ```python
        sortedProds = totalsAndProds.sortBy(lambda t: t[1][1][1])
        sortedProds.collect()
        ```

        이 결과는 `totalsAndProds` RDD에서 상품 이름을 키로 끄집어낸 후 `sortByKey`를 호출해서도 확인할 수 있다. 

    - 정렬 가능한 클래스 생성하기
    - 이차 정렬: 파티션을 2개로 바꾸고, 각 파티션 내에서 요소를 정렬한다.

        ```python
        rdd = sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)])
        rdd2 = rdd.repartitionAndSortWithinPartitions(2)
        rdd2.glom().collect()
        #[[(0, 5), (0, 8), (2, 6)], [(1, 3), (3, 8), (3, 8)]]
        #파티션1: [(0, 5), (0, 8), (2, 6)]
        #파티션2: [(1, 3), (3, 8), (3, 8)]
        ```
        <img width="300" alt="Screen Shot 2021-06-20 at 9 59 42 PM" src="https://user-images.githubusercontent.com/43725183/122675077-d46ba200-d212-11eb-852c-ac15c055cf3c.png">


    - `top`과 `takeOrdered`로 정렬된 요소 가져오기: 이 두 메서드는 전체 데이터를 정렬하지 않는다. 그 대신 각 파티션에서 상위(또는 하위) n개 요소를 가져온 후 이 결과를 병합하고, 이 중 상위(또는 하위) n개 요소를 반환한다. → 두 메서드는 훨씬 더 적은 양의 데이터를 네트워크로 전송하며, sortBy와 take를 개별적으로 호출하는 것보다 무척 빠르다.
- **데이터 그루핑(grouping)**: 데이터 그루핑은 데이터를 특정 기준에 따라 단일 컬렉션으로 집계하는 연산을 의미한다.
    - `aggregateByKey`: 4.1.2절 참조
    - `groupBykey`(`groupBy`): groupByKey는 동일한 키를 가진 모든 요소를 단일 키-값 쌍으로 모은 Pair RDD를 반환한다. groupBy는 PairRDD가 아닌 일반 RDD에서도 사용할 수 있으며, 일반 RDD를 PairRDD로 변환하고 groupByKey를 호출하는 것과 같은 결과를 만들 수 있다. groupByKey는 각 키의 모든 값을 메모리로 가져오기 때문에 이 메서드를 사용할 때는 메모리 리소스를 과다하게 사용하지 않도록 주의해야 한다. 모든 값을 한꺼번에 그루핑할 필요가 없으면 aggregateByKey나 reduceByKey, foldByKey를 사용하는 것이 좋다.
    - `combineByKey`: combineByKey를 호출하려면 세 가지 커스텀 함수를 정의해 전달해야 한다. 첫번째 함수는 `createCombiner`로 각 파티션별로 키의 첫번째 값에서 최초 결합값을 생성하는 데 사용한다. 두번째 함수(`mergeValue`)는 Pair RDD에 저장된 값들을 결합값(combined value)로 병합하고(동일 파티션 내에서 해당 키의 다른 값을 결합 값에 추가해 병합하는데 사용), 세번째 함수(`mergeCombiner`)는 결합 값을 최종 결과를 병합(여러 파티션의 결합 값을 최종 결과로 병합)한다.

        ```python
        def createComb(t):
            total = float(t[5])   #구매 금액
            q = int(t[4])         #구매 수량
            return (total/q, total/q, q, total)  #상품 낱개의 가격을 계산해 최저가격과 최고 가격의 초깃값으로 사용.

        def mergeVal(p,t):
            mn,mx,c,tot = p
            total = float(t[5])
            q = int(t[4])
            return (min(mn,total/q),max(mx,total/q),c+q,tot+total)

        def mergeComb(p1, p2):
            mn1,mx1,c1,tot1 = p1
            mn2,mx2,c2,tot2 = p2
            return (min(mn1,mn1),max(mx1,mx2),c1+c2,tot1+tot2)

        avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb).\
        mapValues(lambda t: (t[0], t[1], t[2], t[3], t[3]/t[2]))
        avgByCust.first()

        totalsAndProds.map(lambda p: p[1]).map(lambda x: ", ".join(x[1])+", "+str(x[0])).saveAsTextFile("ch04/ch04output-totalsPerProd")
        avgByCust.map(lambda (pid, (mn, mx, cnt, tot, avg)): "%d#%.2f#%.2f#%d#%.2f#%.2f" % (pid, mn, mx, cnt, tot, avg)).saveAsTextFile("ch04/ch04output-avgByCust")
        ```
  </details>
  <details close>
  <summary>4.4. Understanding RDD dependencies</summary>
    
* **RDD의 의존 관계(dependency)** : 스파크의 실행 모델은 **Directed Acyclic Graph(DAG)** 에 기반한다. 방향성(directed) 그래프는 간선이 한 정점에서 다른 정점을 가리키는 방향성이 있는 그래프를 의미한다. 그 중 DAG는 간선의 방향을 따라 이동했을 때 같은 정점에 두 번 이상 방문할 수 없도록 연결된 그래프를 의마한다. (이름 그대로 acyclic하다)
**스파크의 DAG는 RDD를 정점(vertex)로, RDD 의존관계를 간선(edge)로 정의한 그래프를 의미한다.** RDD의 변환 연산자를 호출할 때마다 새로운 정점(RDD)와 새로운 간선(의존관계)이 생성된다. 변환 연산자로 생성된 RDD가 이전 RDD에 의존하므로 간선방향은 자식 RDD(새 RDD)에서 부모 RDD(이전 RDD)로 향한다. 이러한 RDD 의존 관계 그래프를 **RDD lineage**라고 한다.
    - **좁은(narrow) 의존 관계**: 데이터를 다른 파티션으로 전송할 필요가 없는 변환 연산은 좁은 의존 관계를 형성한다.
        - one-to-one 의존관계: range 의존관계 이외의 모든 셔플링이 필요하지 않은 변환연산자로 생성.
        - range 의존관계: 여러 부모 RDD에 대한 의존 관계를 하나로 결합한 경우로 union 변환연산자만 해당.
    - **넓은(wide, 또는 shuffle) 의존 관계**: 셔플링을 수행할 때 형성된다. 참고로 RDD를 조인하면 셔플링이 항상 발생한다.<br><br>

    ```python
    import random
    l = [random.randrange(10) for x in range(500)]
    listrdd = sc.parallelize(l, 5)  #파티션 5개로 나누기
    pairs = listrdd.map(lambda x: (x, x*x))     

    reduced = pairs.reduceByKey(lambda v1, v2: v1+v2)

    finalrdd = reduced.mapPartitions(lambda itr: ["K="+str(k)+",V="+str(v) for (k,v) in itr])
    finalrdd.collect()
    print(finalrdd.toDebugString())
    #map은 좁은(one-to-one) 의존 관계를 만드는 연산자. reduceBykey는 넓은(셔플) 의존관계를 형성
    ```

    <img width="1000" alt="Screen Shot 2021-06-20 at 9 55 12 PM" src="https://user-images.githubusercontent.com/43725183/122674936-34157d80-d212-11eb-8c49-cca2da11a64f.png">



    ```
    //print(finalrdd.toDebugString())의 결과

    (5) PythonRDD[5] at collect at <stdin>:1 []
     |  MapPartitionsRDD[4] at mapPartitions at PythonRDD.scala:133 []
     |  ShuffledRDD[3] at partitionBy at NativeMethodAccessorImpl.java:0 []
     +-(5) PairwiseRDD[2] at reduceByKey at <stdin>:1 []
        |  PythonRDD[1] at reduceByKey at <stdin>:1 []
        |  ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:195 []
    ```
    5번 RDD가 가장 최근에 만들어진 RDD. ShuffledRDD가 출력된 지점을 곧 셔플링을 수행하는 시점으로 간주할 수 있다. → 이러한 출력 결과를 잘 활용하면 스파크 프로그램의 셔플링 횟수를 최소화할 수 있다. 
    <br>괄호안 숫자(5)는 해당 RDD의 파티션 개수를 의미한다. 

- **스파크의 stage와 task**: 스파크는 셔플링이 발생하는 지점을 기준으로 스파크 잡(job) 하나를 여러 스테이지(stage)로 나눈다.
    <img width="1000" alt="Screen Shot 2021-06-20 at 9 54 13 PM" src="https://user-images.githubusercontent.com/43725183/122674901-0f210a80-d212-11eb-92c4-cdab59d039b1.png">


    - **1번 stage**는 셔플링으로 이어지는 parallelize, map, reduceByKey 변환 연산들이 포함된다. 1번 스테이지 결과는 중간 파일의 형태로 실행자 머신의 로컬 디스크에 저장된다. **2번 stage**에서는 이 중간 파일의 데이터를 적절한 파티션으로 읽어 들인 후 두 번째 map 변환 연산자부터 마지막 collect 연산까지 실행한다.
    - 스파크는 각 stage와 partition별로 task를 생성해 실행자에게 전달한다. 스테이지가 셔플링으로 끝나는 경우 이 단계의 태스트를 **shuffle-map task**(i.e. 1번 stage의 task)라고 한다. 스테이지의 모든 태스크가 완료되면 드라이버는 다음 스테이지의 태스크를 생성하고 실행자에 전달한다. 이 과정은 마지막 스테이지 결과를 드라이버로 반환할때까지 계속한다. 마지막 스테이지에 생성된 태스크를 **result task**(i.e. 2번 stage의 task)라고 한다.
- **RDD의 체크포인트(checkpoint)**: 변환 연산자를 계속 이어 붙이면 RDD lineage가 제약 없이 길어질 수 있다. 따라서 스파크는 중간에 스냅샷으로 RDD를 스토리지에 보관할 수 있는 **checkpointing** 방법을 제공한다. 일부 노드에 장애가 발생해도 유실된 RDD 조각을 처음부터 다시 계산할 필요가 없고, 그 대신 스냅샷 지점부터 나머지 lineage를 다시 계산한다.
    - 체크포인팅을 실행하면 스파크는 RDD의 데이터뿐만 아니라 RDD의 lineage까지 모두 저장한다. 체크포인팅을 완료한 후에는 저장한 RDD를 다시 계산할 필요가 없으므로 해당 RDD의 의존 관계와 부모 RDD 정보를 삭제한다.
    - 체크 포인팅은 RDD의 checkpoint 메서드를 호출해 실행할 수 있다. 하지만 먼저 SparkContext.setCheckpointDir에 데이터를 저장할 디렉터리부터 지정해야 한다. checkpoint 메서드는 해당 RDD에 job이 실행되기 전에 호출해야 하며, 그 이후 체크포인팅을 실제로 완료하려면 RDD에 행동 연산자(Action) 등을 호출해 잡을 실행하고 RDD를 구체화(materialize)해야한다.
  </details>
  <details close>
  <summary>4.5. Using accumulators and broadcast variables to communicate with Spark executors</summary><br>
    
* **누적 변수(accumulator)** 로 실행자에서 데이터 가져오기: 누적 변수는 여러 실행자가 공유하는 변수로 값을 더하는 연산만 허용한다. 누적 변수는 태스크의 진행상황을 추적하는데 사용할 수 있다.

    ```python
    #accumulators in Python cannot be named
    acc = sc.accumulator(0)  #0은 initial value
    l = sc.parallelize(range(1000000))
    l.foreach(lambda x: acc.add(1))
    acc.value  #누적 변수 값 가져오기. 이 값은 오직 드라이버만 참조할 수 있다. 
    # 1000000  -> 1을 1000000번 더한 결과

    #exception occurs(executor가 참조한 경우)
    l.foreach(lambda x: acc.value)
    #"Accumulator.value cannot be accessed inside tasks"

    #accumulables are not supported in Python
    #accumulableCollections are not supported in Python
    ```

- **공유 변수(broadcast variable)** 로 실행자에 데이터 전송: 공유변수는 여러 클러스터 노드가 공동으로 사용할 수 있는 변수다. 공유변수는 누적변수와 달리 실행자가 수정할 수 없다. 오직 드라이버만이 공유변수를 생성하며, 실행자에서는 읽기 연산만 가능하다.
    - **실행자 대다수가 대용량의 데이터를 공용으로 사용할 때는 이 데이터를 공유 변수로 만드는 것이 좋다.(핵심 point1)** 보통은 드라이버에서 생성한 변수를 태스크에서 사용하면 스파크는 이 변수를 직렬화하고 태스크와 함께 실행자로 전송한다. 하지만 드라이버 프로그램은 동일한 변수를 여러잡에 걸쳐 재사용할 수 있고 잡 하나를 수행할 때도 여러 태스크가 동일한 실행자에 할당될 수 있으므로, 변수를 필요 이상으로 여러번 직렬화해 네트워크로 전송하는 상황이 발생할 수 있다. 이때는 데이터를 더욱 최적화된 방식으로 단 한 번만 전송하는 공유 변수를 사용하는 편이 좋다. (3.2절 예시 참조)
    - 공유 변수는 Broadcast 타입의 객체를 반환하는 SparkContext.broadcast(value) 메서드로 생성한다. value 인수에는 직렬화 가능한 모든 종류의 객체를 전달할 수 있다.
    - **공유변수값을 참조할 때는 항상 value 메서드를 사용해야 한다.(핵심 point2)** 그렇지 않고 공유 변수에 직접 접근하면 스파크는 이 변수를 자동으로 직렬화해 태스크와 함께 전송한다. 이렇게 하면 공유 변수를 사용하는 이유, 즉 성능상 이득을 완전히 잃어버리게 된다.
    - 더 이상 필요하지 않은 공유변수는 destroy를 호출해 완전히 삭제(실행자와 드라이버에서 제거)할 수 있다. 또는 unpersist 메서드를 호출해 공유 변수 값을 실행자의 캐시에서 제거할 수 있다.
    - 공유변수와 관련된 스파크 매개변수
        - `spark.broadcast.compress`: 공유 변수를 전송하기 전에 데이터를 압축할지 여부를 지정한다.
        - `spark.broadcast.blockSize`: 공유 변수를 전송하는데 사용하는 데이터 청크의 크기를 설정한다. 실전 테스트를 거쳐 도출한 기본 값(4096KB)를 그대로 유지하면 좋다.
        - `spark.python.worker.reuse`: 파이썬의 공유 변수 성능에 큰 영향을 주는 매개변수다. 워커를 재사용하지 않으면 각 태스크별로 공유 변수를 전송해야 한다. 기본값이 true를 유지하면 좋다.
  </details></blockquote>
</details>

## 2장 meet the spark family
<details close>
  <summary><b>chapter 5</b> sparkling queries with spark sql</summary>
    <blockquote>
    <details close>
      <summary>5.1. Working with DataFrames </summary>
      
스파크에서는 `DataFrame`으로 **정형 데이터(structured data)**(로우와 칼럼으로 구성되며, 각 컬럼 값이 특정 타입으로 제한된 데이터 구조)를 다룰 수 있다. **데이터 프레임을 만드는 방법**은 세가지가 있다: **(1) 기존 RDD를 변환하는 방법**, (2) SQL 쿼리를 실행하는 방법, (3) 외부 데이터에서 로드하는 방법

- `SparkSession`과 `spark.implicits._` 메서드 임포트하기: 스파크 셸을 시작하면 이 코드를 자동으로 실행하지만, 스파크 독립형 프로그램을 작성할 때는 직접 실행해야 한다.

    ```python
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext

    sc = SparkContext(conf=SparkConf())
    slContext = SQLContext(sc)
    ```

- **데이터 프레임 만들기**(→ (1) 기존 RDD에서 DataFrame 생성하기): 예를들어 로그 파일을 DataFrame으로 가져오려면 먼저 파일을 RDD로 로드해 각 줄을 파싱하고, 로그의 각 항목을 구성하는 하위요소를 파악해야 한다. 이러한 정형화 과정을 거쳐야만 로그 데이터를 DataFrame으로 활용할 수 있다. RDD에서 DataFrame을 만드는 방법은 세가지가 있다.
    1. 로우의 데이터를 튜플 형태로 저장한 RDD를 사용하는 방법

        ```python
        itPostsRows = sc.textFile("first-edition/ch05/italianPosts.csv")
        itPostsSplit = itPostsRows.map(lambda x: x.split("~"))

        #array를 tuple로 만들고, 이것을 toDF()로 dataframe 만들기
        itPostsRDD = itPostsSplit.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12]))
        itPostsDFrame = itPostsRDD.toDF()
        itPostsDFrame.show(10)
        ```

        <img width="1000" alt="Screen Shot 2021-07-29 at 3 58 04 PM" src="https://user-images.githubusercontent.com/43725183/127446251-296f008c-759b-4b60-aab3-95d7ce543391.png">

        ```python
        #column이름을 인자로 전달하기
        itPostsDF = itPostsRDD.toDF(["commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id"])

        #schema 출력하기
        itPostsDF.printSchema() #-> 현재 모든 컬럼의 타입이 String이며 nullable(Null 값을 허용하는 칼럼)이다. 
        ```

    2. 케이스 클래스를 사용하는 방법: RDD의 각 row를 case 클래스로 매핑한 후 toDF 메서드를 호출하는 것이다. 

        ```python
        from pyspark.sql import Row
        from datetime import datetime

        #안전한 변환(문자열을 각 타입으로 변환할 수 없을때 예외를 던지는 대신 None을 반환한다는 의미)
        def toIntSafe(inval):
          try:
            return int(inval)
          except ValueError:
            return None

        def toTimeSafe(inval):
          try:
            return datetime.strptime(inval, "%Y-%m-%d %H:%M:%S.%f")
          except ValueError:
            return None

        def toLongSafe(inval):
          try:
            return int(inval)
          except ValueError:
            return None

        #클래스 매핑하기
        def stringToPost(row):
          #r = row.encode('utf8').split("~")
          r = row.split("~")
          return Row(
            toIntSafe(r[0]),
            toTimeSafe(r[1]),
            toIntSafe(r[2]),
            r[3],
            toIntSafe(r[4]),
            toTimeSafe(r[5]),
            toIntSafe(r[6]),
            toIntSafe(r[7]),
            r[8],
            toIntSafe(r[9]),
            toLongSafe(r[10]),
            toLongSafe(r[11]),
            int(r[12]))

        rowRDD = itPostsRows.map(lambda x: stringToPost(x))
        ```

    3. **스키마를 명시적으로 생성하는 방법**(→ 제일 많이 사용하는 방법): 마지막으로 SparkSession의 createDataFrame 메서드를 사용해보자. 이 메서드는 Row 타입의 객체를 포함하는 RDD와 StuctType 객체를 인자로 전달해 호출할 수 있다. StructType은 스파크 SQL의 테이블 스키마를 표현하는 클래스다.  (DataFrame이 지원하는 데이터 타입은 sql.types를 참조하자)

        ```python
        from pyspark.sql.types import *
        postSchema = StructType([
          StructField("commentCount", IntegerType(), True),
          StructField("lastActivityDate", TimestampType(), True),
          StructField("ownerUserId", LongType(), True),
          StructField("body", StringType(), True),
          StructField("score", IntegerType(), True),
          StructField("creationDate", TimestampType(), True),
          StructField("viewCount", IntegerType(), True),
          StructField("title", StringType(), True),
          StructField("tags", StringType(), True),
          StructField("answerCount", IntegerType(), True),
          StructField("acceptedAnswerId", LongType(), True),
          StructField("postTypeId", LongType(), True),
          StructField("id", LongType(), False)
          ])

        rowRDD = itPostsRows.map(lambda x: stringToPost(x))
        itPostsDFStruct = sqlContext.createDataFrame(rowRDD, postSchema)
        itPostsDFStruct.printSchema()
        ```

        ```python
        #스키마 정보 가져오기
        itPostsDFStruct.columns
        itPostsDFStruct.dtypes
        ```

- 기본 DataFrame API
: DataFrame은 RDD를 기반으로 구현해 "불변성"과 "지연 실행"하는 특징이 있다.
    - 컬럼 선택

        ```python
        postsDf = itPostsDFStruct

        #컬럼 선택
        postsIdBody = postsDf.select("id", "body")
        postsIdBody = postsDf.select(postsDf["id"], postsDf["body"])

        #컬럼 버리기
        postIds = postsIdBody.drop("body")
        ```

    - 데이터 필터링

        ```python
        from pyspark.sql.functions import *

        #Italino라는 단어를 포함하는 포스트 개수를 집계 -> 46
        postsIdBody.filter(instr(postsIdBody["body"], "Italiano") > 0).count()

        #채택된 답변이 없는 질문만 선택
        noAnswer = postsDf.filter((postsDf["postTypeId"] == 1) & isnull(postsDf["acceptedAnswerId"]))

        #limit 함수로는 DataFrame의 상위 n개 row를 선택할 수 있다. 
        firstTenQs = postsDf.filter(postsDf["postTypeId"] == 1).limit(10)
        ```

    - 컬럼 추가 또는 이름 변경

        ```python
        #이름 변경: "ownerUserId"을 "owner"로 변경
        firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")

        #컬럼 추가
        postsDf.filter(postsDf.postTypeId == 1).\
        withColumn("ratio", postsDf.viewCount / postsDf.score).\
        where("ratio < 35").show()
        ```

    - 데이터 정렬: `orderBy`와 `sort` 함수는 동일하다.

        ```python
        postsDf.filter(postsDf.postTypeId == 1).\
        orderBy(postsDf.lastActivityDate.desc()).\
        limit(10).show()
        ```

- SQL 함수로 데이터에 연산 수행: 스파크 SQL 함수는 다음 네가지 카테고리로 나눌 수 있다.
    - **스칼라 함수**: 각 row의 단일 컬럼 또는 여러 컬럼 값을 계산해 단일 값을 반환하는 함수 e.g. abs, exp, substring, length, trim, concat, date_add, year

        ```python
        from pyspark.sql.functions import *
        postsDf.filter(postsDf.postTypeId == 1).\
        withColumn("activePeriod", datediff(postsDf.lastActivityDate, postsDf.creationDate)).\
        orderBy(desc("activePeriod")).head().body.replace("&lt;","<").replace("&gt;",">")
        #<p>The plural of <em>braccio</em> is <em>braccia</em>, and the plural of <em>avambraccio</em> is <em>avambracci</em>.</p><p>Why are the plural of those words so different, if they both are referring to parts of the human body, and <em>avambraccio</em> derives from <em>braccio</em>?</p>
        ```

        head 함수로 첫번째 Row를 드라이버 로컬로 가져온 후 세번째 컬럼인 body의 값을 출력. body 칼럼 내용은 HTML 형식의 테스트여서 가독성을 위해 escape 문자를 다시 특수 문자로 변경함. 

    - **집계 함수**: row의 그룹에서 단일 값을 계산하는 함수. 보통 groupBy와 함께 쓰지만 select이나 withColumn 메서드에 사용하면 전체 데이터셋을 대상으로 집계할 수 있다. e.g. avg, min, max, count, sum

        ```python
        postsDf.select(avg(postsDf.score), max(postsDf.score), count(postsDf.score)).show()
        ```

        <img width="595" alt="Screen Shot 2021-07-29 at 3 58 34 PM" src="https://user-images.githubusercontent.com/43725183/127446334-4960073e-7e78-4e2d-a01b-0bb20bd4116b.png">

    - **윈도 함수**: row의 그룹에서 여러 결과 값을 계산하는 함수. 윈도 함수는 집계 함수와 유사하지만, 로우들을 단일 결과로만 그루핑하지 않는다는 점이 다르다. 윈도 함수를 사용하려면 먼저 집계함수나 아래 표에 나열된 함수 중 하나를 사용해 Column 정의를 구성해야 한다. 그 다음 WindowSpec 객체를 생성하고 이를 Column의 over 함수 인자로 전달한다. over 함수는 이 WindowSpec를 사용하는 윈도 칼럼을 정의해 반환한다.

        ```python
        #example 1
        from pyspark.sql.window import Window

        winDf = postsDf.filter(postsDf.postTypeId == 1).\
        select(postsDf.ownerUserId, postsDf.acceptedAnswerId, postsDf.score, max(postsDf.score).over(Window.partitionBy(postsDf.ownerUserId)).\
        alias("maxPerUser"))

        winDf.withColumn("toMax", winDf.maxPerUser - winDf.score).show(10)
        ```

        ```python
        #example 2
        postsDf.filter(postsDf.postTypeId == 1).\
        select(postsDf.ownerUserId, postsDf.id, postsDf.creationDate, lag(postsDf.id, 1).over(Window.partitionBy(postsDf.ownerUserId).\
        orderBy(postsDf.creationDate)).alias("prev"), lead(postsDf.id, 1).\
        over(Window.partitionBy(postsDf.ownerUserId).\
        orderBy(postsDf.creationDate)).alias("next")).\
        orderBy(postsDf.ownerUserId, postsDf.id).show()
        ```

    - **사용자 정의 함수**: 커스텀 스칼라 함수 또는 커스텀 집계 함수

        ```python
        #example -> 각 질문에 달린 태그의 개수 세기
        countTags = udf(lambda (tags): tags.count("&lt;"), IntegerType())
        postsDf.filter(postsDf.postTypeId == 1).select("tags", countTags(postsDf.tags).alias("tagCnt")).show(10, False)
        ```

- 결측값 다루기: 결측값을 다루는 다양한 방법이 있지만 보통은 (1) null 또는 NaN 등 결측 값을 가진 로우를 DataFrame에서 제외하거나, (2) 결측 값 대신 다른 상수를 채워 넣거나 결측 값에 준하는 특정 칼럼 값을 다른 상수로 치환하는 방법을 사용한다.
    - (방법1) `drop` 사용하기: 인수 없이 호출하면 최소 칼럼 하나 이상에 null이나 NaN 값을 가진 모든 **로우를 DataFrame에서 제외할 수 있다.**

        ```python
        cleanPosts = postsDf.na.drop() #drop("any")를 호출해도 같은 결과!
        cleanPosts.count()

        #drop("all")은 모든 칼럼 값이 null인 row만 제거한다.
        #drop(<column name>) 이렇게 특정 칼럼만 지정할 수도 있다.
        ```

    - (방법2) `fill` 사용하기: 인수를 하나만 지정하면 이 값을 모든 결측값을 대체할 상수로 사용한다. 아래와 같이 특정 컬럼을 지정해서 인자로 전달할 수 도 있다.

        ```python
        postsDf.na.fill({"viewCount": 0}).show()
        ```

    - (방법3) `replace` 사용하기: 다음과 같이 특정 컬럼의 특정 값을 다른 값으로 치환할 수 있다. 아래의 경우 id가 1177번인 데이터를 3000번으로 변경.

        ```python
        postsDf.na.replace(1177, 3000, ["id", "acceptedAnswerId"]).show()
        ```

- DataFrame을 RDD로 변환: `postsRdd = postsDf.rdd`

    → DataFrame API의 내장 DSL, SQL 함수, 사용자 정의 함수 등으로 거의 모든 매핑 작업을 해결할 수 있기 때무에 DataFrame을 RDD로 변환하고 다시 DataFrame으로 만들어야 할 경우는 거의 없다. 

- 데이터 그루핑: groupBy 함수는 `GroupedData` 객체를 반환한다.
    - GroupedData는 표준 집계 함수(count, sum, max, min, avg)를 제공한다.

        ```python
        postsDfNew.\
        groupBy(postsDfNew.ownerUserId, postsDfNew.tags, postsDfNew.postTypeId).\
        count().\
        orderBy(postsDfNew.ownerUserId.desc()).show(10)
        ```

    - agg 함수를 사용해 서로 다른 컬럼의 여러 집계 연산을 한꺼번에 수행할 수도 있다.

        ```python
        postsDfNew.\
        groupBy(postsDfNew.ownerUserId).\
        agg(max(postsDfNew.lastActivityDate), max(postsDfNew.score)).show(10)
        ```

    - 스파크 SQL에 내장된 집계 함수 외에도 커스텀 집계 함수를 직접 정의해 사용할 수 있다.
    - rollup과 cube: groupby는 지정된 칼럼들이 가질 수 있는 값의 모든 조합별로 집계 연산을 수행한다. 반면 rollup와 cube는 지정된 칼럼의 부분 집합을 추가로 사용해 집계 연산을 수행한다.
        - cube: 칼럼의 모든 combination을 대상으로 계산한다.

            ```python
            smplDf.cube(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
            ```

        - rollup: 지정된 칼럼 순서를 고려한 permutation을 사용한다.

            ```python
            smplDf.rollup(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
            ```

- 데이터 조인: DataFrame을 조인하는 방법은 4장에서 살펴본 RDD 조인 방법과 크게 다르지 않다.

    ```python
    #먼저 조인할 데이터를 불러와서 dataframe으로 만들자.
    itVotesRaw = sc.textFile("first-edition/ch05/italianVotes.csv").map(lambda x: x.split("~"))
    itVotesRows = itVotesRaw.map(lambda row: Row(id=long(row[0]), postId=long(row[1]), voteTypeId=int(row[2]), creationDate=datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S.%f")))
    votesSchema = StructType([
      StructField("creationDate", TimestampType(), False),
      StructField("id", LongType(), False),
      StructField("postId", LongType(), False),
      StructField("voteTypeId", IntegerType(), False)
      ])  

    votesDf = sqlContext.createDataFrame(itVotesRows, votesSchema)

    #inner join
    postsVotes = postsDf.join(votesDf, postsDf.id == votesDf.postId)
    #outer join
    postsVotesOuter = postsDf.join(votesDf, postsDf.id == votesDf.postId, "outer")
    ```
    </details>
    <details close>
      <summary>5.2. Beyond DataFrames: introducing DataSets </summary>
      
스파크 2.0부터는 Dataframe을 Dataset의 일종, 즉 Row 객체를 요소로 포함하는 Dataset으로 구현했다. Dataset의 핵심 아이디어는 "사용자에게 도메인 객체에 대한 변환 연산을 손쉽게 표현할 수 있는 API를 지원함과 동시에, 스파크 SQL 실행엔진의 빠른 성능과 높은 안정성을 제공하는 것"이다. 다시말해 일반 자바 객체를 Dataset에 저장할 수 있고, 스파크 SQL의 텅스텐 엔진과 카탈리스트 최적화를 활용할 수 있다는 의미이다. <br>
→ Dataset과 RDD는 중복된 기능을 제공하기 때문에 서로 경쟁 관계라고도 볼 수 있다.
    </details>
    <details close>
      <summary>5.3. Using SQL commands </summary>
      
DataFrame 데이터에 SQL 쿼리를 실행할 수 있는 세 가지(스파크 프로그램, 스파크 SQL 셸, 스파크 Thrift 서버)를 알아보자. 스파크 SQL은 사용자가 작성한 SQL 명령을 DataFrame 연산으로 변환한다. 따라서 SQL 인터페이스에만 익숙한 사용자들에게 스파크 SQL과 DataFrame을 활용할 수 있는 기회를 제공한다. 스파크가 제공하는 SQL 언어는 스파크 전용 SQL과 하이브 쿼리 언어(Hive Query Language, HQL)이다. 
→ 스파크 커뮤니티는 더 풍부한 기능을 제공하는 HQL을 권장한다. 스파크에서 하이브 기능을 사용하려면 SparkSession을 구성할 때 SparkSession.Builder 객체의 enableHiveSupport 메서드를 호출해야 한다. (하이브를 지원하는 스파크 셸을 사용하면 셸이 자동으로 하이브 기능을 활성화 한다)

```python
spark = SparkSession.builder().enableHiveSupport().getOrCreate()
```

1. 테이블 카탈로그와 하이브 메타 스토어
    - **테이블 카탈로그(Table catalog)와 하이브 메타 스토어**: DataFrame을 테이블로 등록하면 스파크는 사용자가 등록한 테이블 정보를 테이블 카탈로그(table catalog)에 저장한다. 
    하이브 지원 기능이 없는 스파크에서는 테이블 카탈로그를 단순한 인-메모리 Map으로 구현한다. 따라서 등록된 테이블 정보를 드라이버의 메모리에만 저장하고, 스파크 세션이 종료되면 같이 사라진다. 반면 하이브를 지원하는 SparkSession에서는 테이블 카탈로그가 하이브 메타스토어를 기반으로 구현되었다. 하이브 메타스토어는 영구적인 데이터베이스로, 스파크 세션을 종료하고 새로 시작해도 DataFrame 정보를 유지한다.
    - 테이블 임시 등록: 하이브를 지원하는 스파크에서도 임시로 테이블을 저장할 수 있다. 그러고 나면 SQL 인터페이스에서 posts_temp 이름을 참조해 데이터에 질의를 할 수 있다.

        ```python
        postsDf.registerTempTable("posts_temp")
        ```

    - 테이블 영구 등록: HiveContext는 metastore_db 폴더 아래 로컬 작업 디렉터리에 Derby 데이터베이스를 생성한다(데이터베이스가 이미 존재하면 이를 재사용한다.) 작업 디렉토리 위치를 변경하려면 hive-site.xml 파일의 hive.metastore.warehouse.dir에 원하는 경로를 지정한다.

        ```python
        postsDf.write.saveAsTable("posts")
        votesDf.write.saveAsTable("votes")
        ```

    - 스파크 테이블 카탈로그

        ```python
        spark.catalog.listTables().show()  #현재 등록된 테이블 목록 조회
        #isTemporary: 어떤 테이블이 영구 테이블이고 임시 테이블인지 확인할 수 있음
        #tableType: MANAGED는 스파크가 해당 테이블의 데이터까지 관리한다는 것을 의미함. EXTERNAL은 데이터를 다른 시스템(e.g. RDBMS)로 관리한다는 뜻.
        ```

        테이블 정보는 메타스토어 데이터베이스에 등록된다. 데이터베이스의 MANAGED 테이블은 사용자의 홈 디렉토리 아래 spark_warehouse 폴더에 저장된다. 저장 위치를 변경하려면 spark.sql.warehouse.dir 매개변수에 원하는 위치를 설정한다. 

        - 특정 테이블의 칼럼 정보 조회: `spark.catalog.listColumns("votes").show()`
        - SQL 함수 목록 조회: `spark.catalog.listFunctions.show()`
        - 테이블을 캐시하거나 삭제: `cacheTable`, `uncacheTable`, `isCached`, `clearCache`
    - 원격 하이브 메타스토어 설정: 스파크가 원격지의 하이브 메타스토어 데이터베이스를 사용하도록 설정하는 것도 가능하다. 스파크의 원격지 메타스토어로는 기존에 설치한 하이브 메타스토어 데이터베이스를 활용하거나, 스파크가 단독으로 사용할 새로운 데이터베이스를 구축할 수도 있다. 하이브 메타스토어는 스파크 conf 디렉터리 아래에 있는 하이브 설정 파일(hive-site.xml)의 매개변수들로 설정하며, 이 파일을 설정하면 spark.sql.warehouse.dir 매개변수를 오버라이드된다. 스파크가 원격 하이브 메타 스토어를 사용하도록 설정하려면 다음 property들을 hive-site.xml 파일에 추가해야 한다. (property는 configuration 태그 내에 여러개 넣을 수 있다. 각 property tag은 name, value tag로 구성되어 있다)
        - `javax.jdo.option.ConnectionURL`: JDBC 접속 URL
        - `javax.jdo.option.ConnectionDriverName`: JDBC 접속 URL
        - `javax.jdo.option.ConnectionUserName`: JDBC 접속 URL
        - `javax.jdo.option.ConnectionPassword`: 데이터베이스 사용자 암호
2. SQL 쿼리 실행: 앞서서 DataFrame을 테이블로 등록하고 나면 이제 SQL 표현식을 사용해 이 데이터에 질의를 실행할 수 있다. 다음과 같이 `sql` 함수를 사용해서 SQL query를 날린다. 쿼리는 하이브 언어 매뉴얼을 참조하자.([https://cwiki.apache.org/confluence/display/Hive/LanguageManual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual))

    ```python
    resultDf = sqlContext.sql("select * from posts")
    #resultDf의 type은 DataFrame이다. 
    ```

    cf. 스파크 SQL 셸 사용하기: 스파크는 스파크 셸 외에도 별도의 SQL 셸을 제공한다. `spark-sql`명령으로 실행하며, spark-shell 및 spark-submit의 인수들을 똑같이 전달할 수 있다. spark-sql 명령을 시작할 때 인수를 전달하지 않으면 SQL 셸을 로컬 모드로 시작한다. (+ query 마지막에 꼭 세미 콜론 붙이기!)

    ```python
    spark-sql> select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3;

    # -e 인수를 사용하면 스파크 SQL 셸에 들어가지 않고도 SQL 쿼리를 실행할 수 있다. 
    $ spark-sql -e "select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3"

    # -f: 파일에 저장된 SQL 명령을 실행한다.
    # -i: 초기화 SQL 파일을 지정할 수 있다. 이 초기화 SQL 파일에는 다른 SQL 명령을 실행하기 전에 우선적으로 실행할 명령들을 입력한다. 
    ```

3. 쓰리프트 서버로 스파크 SQL 접속

    스파크 쓰리프트라는 JDBC( 또는 ODBC) 서버를 이용해 원격지에서 SQL 명령을 실행할 수 있다. JDBC( 또는 ODBC)는 관계형 데이터베이스의 표준 접속 프로토콜이므로 쓰리프트 서버를 이용해 관계형 데이터베이스와 통신할 수 있는 모든 애플리케이션에서 스파크를 사용할 수 있다. 

    쓰리프트 서버는 여러 사용자의 JDBC 및 ODBC 접속을 받아 사용자의 쿼리를 스파크 SQL 세션으로 실행하는 독특한 스파크 애플리케이션이다. 쓰리프트 서버로 전달된 SQL 쿼리는 dataFrame으로 변환한 후 최종적으로 RDD 연산으로 변환해 실행하며, 실행 결과는 다시 JDBC 프로토콜로 반환한다. 

    (자세한 내용은 생략)
    </details>
    <details close>
      <summary>5.4. Saving and loading DataFrame data </summary>
      
다양한 소스의 외부 데이터를 로드하고 저장하는 방법을 알아보자.

1. 기본 데이터 소스
    - 데이터 소스: 스파크는 기본 파일 포맷 및 데이터베이스를 지원한다. 스파크에서는 이를 데이터 소스라고 한다. 데이터 소스에는 JDBC와 하이브를 비롯해 JSON, ORC, Parquet 파일 포맷등이 해당된다. 또 스파크는 MySQL 및 PostgreSQL 관계형 데이터베이스와 스파크를 연동하는 Dialect 클래스도 제공한다.
    - **JSON** : 외부시스템과 데이터를 주고받는 포맷으로 매우 적합하다. 또한 포맷이 간단하고 사용이 간편하며 사람이 읽을 수 있는 형태로 데이터를 저장한다는 장점이 있다. 그러나 데이터를 영구적으로 저장하는 데 사용하기에는 저장 효율이 떨어진다는 단점이 있다.
    - **ORC(Optimized Row Columnar)**: 하둡의 표준 데이터 저장 포맷인 RCFile 보다 효율적을 하이브의 데이터를 저장하도록 설계된 파일 포맷이다.([https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC))
        - ORC는 칼럼형 포맷이다. 칼럼형 포맷은 각 로우의 데이터를 순차적으로 저장하는 로우 포맷과 달리 각 칼럼의 데이터를 물리적으로 가까운 위치에 저장하는 방식을 의미한다.
        - ORC는 로우 데이터를 **스트라이프(stripe)** 여러 개로 묶어서 저장하며, 파일 끝에는 파일 푸터(file footer)와 포스트 스크립트(postscript) 영역이 있다. **파일 푸터**에는 파일의 스트라이프 목록과 스트라이프별 로우 개수, 각 컬럼의 데이터 타입을 저장한다. **포스트 스크립트** 영역에는 파일 압축과 관련된 매개변수들과 파일 푸터의 크기를 저장한다. 스트라이프의 기본 크기는 250MB이다.
    - **Parquet**: ORC와 마찬가지로 Parquet도 칼럼형 파일 포맷이며 데이터를 압축할 수 있다. 스파크는 Parquet을 기본 데이터 소스로 사용한다.
2. 데이터 저장
    - Writer 설정: DataFrameWriter의 각 설정 함수가 현재가지 설정된 DataFrameWriter에 자신에게 전달된 설정을 덧붙여 다시 DataFrameWriter 객체를 반환하므로, 원하는 설정을 하나씩 차례대로 쌓아서 전체 설정 항목을 점진적으로 추가할 수 있다. 다음은 DataFrameWriter 설정함수다.
        - format: 데이터를 저장할 파일 포맷. 기본 데이터 소스(json, parquet, orc)나 커스텀 데이터 소스 이름을 사용할 수 있다.
        - mode: 지정된 테이블 또는 파일이 이미 존재하면 이에 대응해 데이터를 저장할 방식을 지정한다. overwrite(기존 데이터를 덮어쓴다), append(기존 데이터에 추가한다), ignore(아무것도 하지않고 저장 요청을 무시한다.), error(예외를 던진다) 중 하나를 지정할 수 있으며, 기본으로는 error가 사용된다.
        - option과 options: 데이터 소스를 설정할 매개변수 이름과 변수 값을 추가한다.(options는 매개변수의 이름-값 쌍을 Map 형태로 전달한다)
        - partitionBy: 복수의 파티션 칼럼을 지정한다.

        ```python
        #example
        postsDf.write.format("json").saveAsTable("postsjson")
        ```

    - saveAsTable: 하이브 테이블로 저장, 테이블을 하이브 메타스토어에 등록한다. 저장을 완료하고 하이브 메타스토어를 사용해 이 테이블에 질의를 실행할 수 있다.

        ```python
        sqlContext.sql("select * from postsjson")
        ```

    - insertInto: 하이브 테이블로 저장. 이 경우는 이미 하이브 메타스토어에 존재하는 테이블을 지정해야 하며 이 테이블과 새로 저장할 DataFrame 스키마가 서로 같아야 한다.
    - save: 데이터를 파일에 저장. 데이터를 저장할 경로를 전달하면 스파크 실행자가 구동중인 모든 머신의 로컬 디스크에 파일을 저장한다.
    - 이 외에 단축 메서드로 데이터를 저장하거나 jdbc 메서드로 관계형 데이터베이스에 데이터를 저장하는 것도 가능하다.

        ```python
        props = {"user": "user", "password": "password"}
        postsDf.write.jdbc("jdbc:postgresql:#postgresrv/mydb", "posts", properties=props)
        ```

3. 데이터 불러오기
    - DataFrameReader 사용하기: DataFrameReader는 DataFrameWriter와 거의 사용방법이 동일하다.(save 대신 load 사용) 설정함수로 format, option, options를 사용할 수 있다. 반면 DataFrameWriter와 달리 schema 함수로 DataFrame 스키마를 지정할 수 있다. 스파크는 데이터 소스의 스키마를 대부분 자동으로 감지하지만, 스키마를 직접 지정하면 그만큼 연산 속도를 높일 수 있다.

        또한 table 함수를 사용해 하이브 메타 스토어에 등록된 테이블에서 DataFrame을 불러올 수 있다.

        ```python
        postsDf = sqlContext.read.table("posts")
        postsDf = sqlContext.table("posts")
        ```

    - jdbc 메서드로 관계형 데이터베이스에서 데이터 불러오기: DataFrameReader jdbc 함수는 DataFrameWriter jbdc 함수와는 다르게 여러 조건을 사용해 DataFrame으로 불러올 데이터셋의 범위를 좁힐 수 있다

        ```python
        #example: PostgreSQL 테이블에서 최소 조회 수를 세번 이상 기록한 포스트 불러오기
        result = sqlContext.read.jdbc("jdbc:postgresql:#postgresrv/mydb", "posts", predicates=["viewCount > 3"], properties=props)
        ```

    - sql 메서드로 등록한 데이터 소스에서 데이터 불러오기: 임시 테이블을 등록하면 SQL 쿼리에서도 기존 데이터 소스를 참조할 수 있다.

        ```python
        sqlContext.sql("CREATE TEMPORARY TABLE postsjdbc "+
          "USING org.apache.spark.sql.jdbc "+
          "OPTIONS ("+
            "url 'jdbc:postgresql:#postgresrv/mydb',"+
            "dbtable 'posts',"+
            "user 'user',"+
            "password 'password')")

        sqlContext.sql("CREATE TEMPORARY TABLE postsParquet "+
          "USING org.apache.spark.sql.parquet "+
          "OPTIONS (path '/path/to/parquet_file')")
        resParq = sql("select * from postsParquet")
        ```
    </details>
     <details close>
      <summary>5.5. Catalyst optimizer </summary>
       
- **카탈리스트 최적화 엔진(catalyst optimizer)**: DataFrame DSL과 SQL 표현식을 하위 레벨의 RDD 연산으로 변환한다. 사용자는 카탈리스트를 확장해 다양한 최적화를 추가로 적용할 수 있다.

    <img width="616" alt="Screen Shot 2021-07-29 at 5 52 29 PM" src="https://user-images.githubusercontent.com/43725183/127462469-d86afebc-e131-40c5-9487-faf6e5429001.png">

    이 그림은 카탈리스트 엔진을 최적화하는 과정 전반을 도식화한 것이다. 
    카탈리스트는 먼저 **(1) parsed logical plan**을 생성한다.  
    → 쿼리가 참조하는 테이블 이름, 칼럼 이름, 클래스 고유 이름등의 존재여부를 검사하고, (2) **analyzed logical plan**을 생성한다. 
    → 하위 레벨 연산을 재배치하거나 결합하는 등 여러 방법으로 실행 계획의 최적화를 시도한다. (e.g. 조인할 데이터양을 줄이려고 조인 연산 다음에 사용한 필터링 연산을 조인 앞으로 옮기기도 한다) 최적화 단계를 완료 하면 (3) **optimized logical plan**을 생성한다. 
    → 마지막으로 카탈리스트는 optimized logical plan에서 실제 (4) **physical plan**을 작성한다. 

- 실행계획 검토: DataFrame의 explain 메서드를 사용해 최적화 결과를 확인하고 실행 계획을 검토할 수 있다. 또한 스파크 웹 UI의 SQL 탭에서 각 쿼리의 Details 컬럼에 있는 +details를 클릭하면 explain과 결과를 유사하게 출력한다.

    ```python
    postsRatio = postsDf.filter(postsDf.postTypeId == 1).withColumn("ratio", postsDf.viewCount / postsDf.score)
    postsFiltered = postsRatio.where(postsRatio.ratio < 35)
    postsFiltered.explain(True)
    ```

- 파티션 통계 활용: 카탈리스트는 DataFrame의 파티션 내용을 검사하고 각 컬럼의 통계를 계산한 후 이를 활용해 필터링 작업 중 일부 파티션을 건너뛰고 작업 성능을 추가로 최적화한다. 칼럼 통계는 DataFrame을 메모리에 캐시하면 자동으로 계산되므로 사용자가 특별히 해야할 일은 따로 없다. 단지 DataFrame을 메모리에 캐시하는 것이 성능상 좋다는 점만 기억하자.
    </details>
    <details close>
      <summary>5.6. Performance improvements with Tungsten </summary>
      
텅스텐 프로젝트는 스파크의 메모리 관리 방식을 완전히 개혁했으며, 정렬, 집계, 셔플링 연산의 성능도 대폭 개선했다. 스파크 버전 1.5부터는 텅스텐을 기본으로 활성화한다. 
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 6</b> ingesting data with spark streaming</summary>
  <blockquote>
    <details close>
      <summary>6.1. Writing Spark Streaming applications </summary>
      
- 스파크의 **미니 배치(mini-batch)** 생성
    - 스파크 스트리밍은 특정 시간 간격 내에 유입된 데이터 블록을 RDD로 구성한다. 그림에서 볼 수 있듯이 다양한 외부 시스템 데이터를 스파크 스트리밍 잡으로 입수할 수 있다. 여기서 말하는 외부 시스템은 단순한 파일 시스템이나 TCP/IP 접속 외에도 카프카, 플럼, 트위터, 아마존 Kinesis 같은 분산 시스템을 의미한다.
    - 스파크 스트리밍은 각 데이터 소스별로 별도의 리시버를 제공한다. 리시버에는 해당 데이터 소스에 연결하고 데이터를 읽어 들여 스파크 스트리밍에 전달하는 로직이 구현되어 있다. 스파크 스트리밍은 리시버가 읽어 들인 데이터를 미니배치 RDD로 분할하며, 스파크 애플리케이션은 이 미니배치 RDD를 애플리케이션에 구현된 로직(e.g. 머신러닝)에 따라 처리한다.
    - 미니배치를 처리한 결과는 파일 시스템이나 관계형 데이터베이스 또는 다른 분산 시스템으로 내보낼 수 있다. 이제 예제를 통해 스파크 스트리밍을 알아보자.

    <img width="503" alt="Screen Shot 2021-07-29 at 5 55 11 PM" src="https://user-images.githubusercontent.com/43725183/127462888-db75e803-f327-4b53-8054-60c2d028321b.png">

1. 스트리밍 컨텍스트 생성
    - 어떤 클러스터를 사용하든 반드시 코어를 두 개 이상 실행자에 할당해야 한다. 스파크 스트리밍의 각 리시버가 입력 데이터 스트림을 처리하려면 코어(엄밀히 말하면 thread)를 각각 한 개씩 사용해야 하며, 별도로 최소 코어 한 개 이상이 프로그램 연산을 하는 데 필요하다.
    - StreamingContext 인스턴스 만들기: 지금은 우선 Duration을 5초로 설정했다.

        ```python
        from pyspark.streaming import StreamingContext

        ssc = StreamingContext(sc, 5)
        #SparkContext 객체와 Duration을 인자로 넣어준다. 
        ```

2. 파일의 스트림 데이터 읽고 저장하기.
    - StreamingContext의 `textFileStream`: 지정된 디렉터리를 모니터링하고, 디렉터리에 새로 생성된 파일을 개별적으로 읽어들인다. textFileStream이 새로 생성된 파일을 읽어 들인다는 것은 StreamingContext를 시작할 시점에 이미 폴더에 있던 파일은 처리하지 않는다는 것을 의미한다. (+ 파일에 데이터를 추가해도 읽어들이지 않는다.)
    → 리눅스 쉘 스크립트(`splitAndSend.sh`)로 스트리밍 데이터를 임의로 생성하자. 이 스크립트는 압축을 해제한 데이터 파일을 파일 50개로 분할한 후, 분할한 파일을 HDFS 디렉터리에 3초 주기로 하나씩 복사한다. (실제 운영 환경에서도 이러한 방식으로 데이터를 전송할 때가 많다)
    - 입력폴더 지정: `filestream = ssc.textFileStream("/home/spark/ch06input")`
    → `filestream`은 DStream 클래스이다. RDD와 마찬가지로 DStream은 다른 DStream으로 변환하는 다양한 메서드를 제공한다. e.g. 필터링, 매핑, 리듀스, 조인 등
    - 실습 예제: map 대신 flatMap을 사용하는 이유는 포맷이 맞지 않는 데이터를 건너뛰기 위함이다.

        ```python
        from datetime import datetime
        def parseOrder(line):
          s = line.split(",")
          try:
              if s[6] != "B" and s[6] != "S":
                raise Exception('Wrong format')
              return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],
              "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]
          except Exception as err:
              print("Wrong line format (%s): " % line)
              return []

        orders = filestream.flatMap(parseOrder)

        from operator import add
        numPerType = orders.map(lambda o: (o['buy'], 1L)).reduceByKey(add)
        ```

    - 결과를 파일로 저장: saveAsTextFiles는 prefix 문자열과 suffix 문자열을 받아 데이터를 주기적으로 저장할 경로를 구성한다. 각 미니배치 RDD의 데이터는 <prefix>-<밀리초 단위 시각>.<접미 문자열>  폴더에 저장된다. 즉, 미니배치 RDD의 주기(e.g. 5초)마다 새로운 디렉터리를 생성한다는 의미이다.

        ```python
        numPerType.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")
        ```

    - 스트리밍 계산 작업의 시작과 종료

        ```python
        ssc.start()  #(1) 스트리밍 컨텍스트 시작
        ```

        ```bash
        #(2)스트리밍 데이터 생성
        $ chmod +x ch06/splitAndSend.sh
        $ mkdir /home/spark/ch06input
        $ cd ch06
        $ ./splitAndSend.sh /home/spark/ch06input local  #local 파일 시스템의 폴더를 사용할때는 반드시 local 인수를 추가해야 한다. 
        ```

        스크립트는 orders.txt 파일을 분할하고 지정된 폴더로 하나씩 복사한다. 스트리밍 애플리케이션은 이 파일들을 자동으로 읽어 들여 주문 건수를 집계한다. 

        ```python
        #(3)실행중인 스트리밍 컨텍스트를 셸에서 중지하기.
        ssc.stop(False)  #False -> 스파크 컨텍스트는 중지x
        ```

    - 결과 확인: 여러 파일(part-00000)로 나눠져 있어도 *를 사용하면 하나의 객체에 모두 읽을 수 있다.

        ```bash
        allCounts = sc.textFile("/home/spark/ch06output/output*.txt")
        ```

3. 시간에 따라 변화하는 계산 상태 저장

- 스파크 스트리밍에서는 계산 상태를 갱신하는 여러 메서드를 사용해 과거 데이터와 현재 미니배치의 새로운 데이터를 결합하고, 훨씬 더 강력한 스트리밍 프로그램을 만들 수 있다.

    <img width="471" alt="Screen Shot 2021-07-29 at 5 54 27 PM" src="https://user-images.githubusercontent.com/43725183/127462789-916d55ce-265d-480f-ae3e-ae536de83c79.png">

- 과거의 계산 상태를 현재 계산에 반영할 수 있는 `updateStateBykey`와 `mapWithState` 메서드를 제공한다. 두 메서드 모두 키-값 튜플로 구성된 Pair DStream에서만 이 메서드를 사용할 수 있다.
    - `updateStateBykey`

        ```python
        #step 1
        #Pair DStream 생성: 키(고객 ID), 값(거래액 = 주문 수량*매매가격)
        amountPerClient = orders.map(lambda o: (o['clientId'], o['amount']*o['price']))

        #step 2
        #amountPerClient DStream에 updateStateByKey 메서드를 적용해 StateDStream을 생성
        amountState = amountPerClient.updateStateByKey(lambda vals, totalOpt: sum(vals)+totalOpt if totalOpt != None else sum(vals))
        # 이 키의 상태가 이미 존재할 때는 상태 값에 새로 유입된 값의 합계를 더한다. 반면 이전 상태 값이 없을 때는 새로 유입된 값의 합계만 반환한다. 

        #step 3
        #거래액 1~5위 고객 계산 -> DStream의 각 RDD를 정렬한 후 각 RDD에서 상위 다섯개 요소만 남김.
        top5clients = amountState.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))

        #step 4
        #초당 거래 주문 건수와 거래액 1~5위 고객 결과를 배치 간격당 한 번씩만 저장하려면, 두 결과 DStream을 단일 DStream으로 병합해야 한다. 
        #join, cogroup, union 사용가능. 
        # 먼저 요소타입을 동일하게 맞춤
        buySellList = numPerType.map(lambda t: ("BUYS", [str(t[1])]) if t[0] else ("SELLS", [str(t[1])]) )
        top5clList = top5clients.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5CLIENTS", arr))

        #그리고 두 DStream 합침
        finalStream = buySellList.union(top5clList)

        #step 5
        #병합된 파일 저장
        finalStream.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")

        #step 6
        #updateStateByKey 메서드를 사용할 때는 메서드가 반환하는 State DStream에 체크포인팅을 반드시 적용해야 한다. 
        sc.setCheckpointDir("/home/spark/checkpoint/")
        ```

    - `mapWithState` : updateStateByKey와 mapWithState의 가장 큰 차이점은 상태 값의 타입과 반환 값의 타입을 다르게 적용할 수 있다는 것이다.

        `mapWithState`는 기능뿐만 아니라 성능 면에서도 우수하다. `mapWithState`는 `updateStateByKey` 보다 키별 상태를 열 배 더 많이 유지할 수 있으며, 처리 속도는 여섯배나 빠르다. → 자세한 내용은 생략

4. 전체 코드 실행: 스트리밍 컨텍스트를 시작한 후 `splitAndSend.sh` 스크립트를 재실행하면 출력폴더와 part-00000 파일이 생성된다. 

```python
from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, 5)
filestream = ssc.textFileStream("/home/spark/ch06input")

from datetime import datetime
def parseOrder(line):
  s = line.split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],
      "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]
  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []

orders = filestream.flatMap(parseOrder)
from operator import add
numPerType = orders.map(lambda o: (o['buy'], 1L)).reduceByKey(add)

amountPerClient = orders.map(lambda o: (o['clientId'], o['amount']*o['price']))

amountState = amountPerClient.updateStateByKey(lambda vals, totalOpt: sum(vals)+totalOpt if totalOpt != None else sum(vals))
top5clients = amountState.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))

buySellList = numPerType.map(lambda t: ("BUYS", [str(t[1])]) if t[0] else ("SELLS", [str(t[1])]) )
top5clList = top5clients.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5CLIENTS", arr))

finalStream = buySellList.union(top5clList)

finalStream.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")

sc.setCheckpointDir("/home/spark/checkpoint/")

ssc.start()
```

5. 윈도 연산: 일정 시간동안 유입된 데이터만 계산하기. 윈도 연산은 미니배치의 슬라이딩 윈도(sliding window)를 기반으로 수행한다. 스파크 스트리밍은 슬라이딩 윈도의 길이(window duration)와 이동거리(slide duration)즉, 윈도 데이터를 얼마나 자주 계산할지)를 바탕으로 윈도 DStream을 생성한다. 슬라이딩 윈도의 길이와 이동 거리는 반드시 미니배치 주기의 배수여야 한다.  → 결과는 윈도우당 하나씩  계산된다. 

<img width="529" alt="Screen Shot 2021-07-29 at 5 53 58 PM" src="https://user-images.githubusercontent.com/43725183/127462697-588c8d91-4fa6-49d4-9696-abb15bebe51e.png">

- 예제: 지난 한 시간동안 거래된 각 유가 증권별 거래량을 집계해야 한다. (→ 윈도 길이가 1시간) 이동거리는 각 미니배치 별로 계산해야 하기 때문에 미니배치 주기와 동일하게 5초로 설정한다.
- 윈도 DStream 생성하기
    - `reduceByKeyAndWindow`: 리듀스 함수와 윈도 길이를 지정한다. 이 메서드는 윈도 DStream을 생성하고 데이터를 리듀스 함수에 전달한다. reduceByKeyAndWindow 대신 아래와 같이 window 메서드를 호출한 결과에 reduceByKey 메서드를 호출해도 같은 결과를 얻을 수 있다.

        ```python
        stocksWindow = orders.map(lambda x: (x['symbol'], x['amount'])).window(60*60)
        stocksPerWindow = stocksWindow.reduceByKey(add)

        topStocks = stocksPerWindow.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).map(lambda x: x[0]).\
        zipWithIndex().filter(lambda x: x[1] < 5)).repartition(1).\
        map(lambda x: str(x[0])).glom().\
        map(lambda arr: ("TOP5STOCKS", arr))
        ```

    - 다른 윈도 연산자

        <img width="463" alt="Screen Shot 2021-07-29 at 5 53 33 PM" src="https://user-images.githubusercontent.com/43725183/127462631-e88b382e-8d19-44b5-8543-7422a31bdcf6.png">

6. 그 외 내장 입력 스트림: 스파크 스트리밍에는 textFileStream 외에도 다양한 메서드로 데이터를 수신하고 DStream을 생성할 수 있다.

- 파일 입력 스트림: `binaryRecordsStream`, `fileStream`이 있다. `textFileStream`과 마찬가지로 특정 폴더 아래 새로 생성된 파일들을 읽어들인다. 차이점은 텍스트 외 다른 유형의 파일을 읽을 수 있다는 것이다.
- 소켓 입력 스트림: 스파크 스트리밍을 사용해 TCP/IP 소켓에서 바로 데이터를 수신할 수도 있다. `socketStream` 과 `socketTextStream`이 있다.
    </details>
    <details close>
      <summary>6.2. Using external data sources </summary>
      
지금까지 스파크 스트리밍의 내장 데이터 소스(파일 및 소켓)을 사용하는 방법을 살펴보았다. 이제 외부 데이터 소스에 연결해보자. 스파크가 공식적으로 커넥터를 지원하는 외부시스템 및 프로토콜에는 **카프카(kafka), 플럼(flume), 아마존 kinesis, 트위터, ZeroMQ, MQTT**가 있다. 
→ 이 절에서는 매매주분 데이터를 파일에서 직접 읽어들이는 대신 또 다른 셸 스크립트를 사용해 파일의 주문 데이터를 카프카 토픽으로 전송한다. 스파크 스트리밍 애플리케이션은 이 토픽에서 주문 데이터를 읽어 들이고, 각 지표의 계산 결과를 다시 또 다른 카프카 토픽으로 전송한다. 그런 다음 카프카의 컨슈머 스크립트(kafka-console-consumer.sh)를 사용해 지표 결과를 수신하고 출력한다. 

1. 카프카 시작
    - 카프카 설치: 카프카 버전은 반드시 스파크 버전과 호환되는 것을 선택한다.

        ```bash
        #binary file 다운로드 https://kafka.apache.org/downloads
        $ tar -xvfz kafka_2.12-2.8.0.tgz
        ```

    - 주키퍼 시작하기: 카프카는 아파치 주키퍼를 사용한다. 주키퍼는 분산 프로세스를 안정적으로 조율할 수 있는 오픈소스 서버 소프트웨어이다.

        ```bash
        $ cd ~/bin/kafka_2.12-2.8.0
        $ bin/zookeeper-server-start.sh config/zookeeper.properties & 
        #port 2181번
        ```

    - 카프카 시작

        ```bash
        $ bin/kafka-server-start.sh config/server.properties & 
        ```

    - 거래 주문 데이터를 전송할 토픽(orders)와 지표 데이터를 전송할 토픽(metrics)를 생성하자.

        ```bash
        $ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic orders
        $ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic metrics
        $ bin/kafka-topics.sh --list --zookeeper localhost:2181
        #metrics
        #orders
        ```

2. 카프카를 사용해 스트리밍 애플리케이션 개발
    - 셸 시작하기: (1) 카프카 라이브러리와 (2) 스파크-카프카 커넥터 라이브러리를 스파크 셸의 클래스 패스에 추가해 시작하자. 각 라이브러리의 JAR 파일을 지겁 내려받을 수도 있지만, 다음과 같이 packages 매개변수를 지정하면 스파크는 JAR 파일을 자동으로 내려받아 사용한다.

        ```bash
        $ spark-shell --master local[4] \
        --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,\
        org.apache.kafka:kafka_2.11:0.8.2.1
        ```

        반면 스파크 독립형 애플리케이션을 메이븐 프로젝트로 빌드할 때는 애플리케이션의 pom.xml 파일에 다음 의존 라이브러리를 추가하면 된다. 

        ```xml
        <dependency>
        	<groupId>org.apache.spark</groupId>
        	<artifactId>spark-streaming-kafka-0-8_2.11</artifactId>
        	<version>2.0.0</version>
        </dependency>
        <dependency>
        	<groupId>org.apache.kafka</groupId>
        	<artifactId>kafka_2.11</artifactId>
        	<version>0.8.2.1</version>
        </dependency>
        ```

    - 스파크-카프카 커넥터 사용
        - 스파크-카프카 커넥터에는 (1) 리시버 기반 커넥터(receiver-based connector)와 (2) 다이렉트 커넥터(direct connector)가 있다.
            - **리시버 기반 커넥터(receiver-based connector)**: 간혹 메시지 한 개를 여러번 읽어오기도 한다. 또한 연산 성능이 다소 떨어진다는 단점이 있다.
            - **다이렉트 커넥터(direct connector)**: 입력된 메시지를 정확히 한번 처리함.
        - 카프카 설정에 필요한 매개변수 설정: 카프카 토픽의 데이터를 읽어 DStream을 생성하려면, 먼저 카프카 설정에 필요한 매개변수들을 Map 객체로 구성해야 한다. 이 Map에는 최소한 카프카 브로커(broker) 주소를 가리키는 metadata.broker.list 매개변수를 반드시 포함해야 한다.
        - 위의 Map 매개변수와 스트리밍 컨텍스트 객체, 그리고 접속할 카프카 토픽 이름을 담은 Set 객체를 KafkaUtils.createDirectStream 메서드에 전달한다. 또 메시지의 키 클래스, 값 클래스, 키의 디코더 클래스, 값의 디코더 클래스를 createDirectStream 메서드의 타입 매개 변수로 전달해야 한다. 예제에서는 키와 값이 문자열 타입이므로 카프카의 StringDecoder 클래스를 키와 값의 디코더로 사용할 수 있다.

            ```python
            kafkaReceiverParams = {"metadata.broker.list": "192.168.10.2:9092"}
            kafkaStream = KafkaUtils.createDirectStream(ssc, ["orders"], kafkaReceiverParams)
            ```

        - 리시버 기반 커넥터는 마지막으로 가져온 메시지의 오프셋을 주키퍼에 저장하지만 다이렉트 커넥터는 주키퍼 대신 스파크 체크포인팅 디렉터리에 오프셋을 저장한다. 마지막으로 가져온 메시지의 오프셋이 없을 경우 auto.offset.reset 매개변수로 어떤 메시지부터 가져올지 지정할 수 있다(Map 매개변수에 같이 저장하면 된다). 이 값을 smallest로 설정하면 가장 작은 오프셋의 데이터부터 가져온다. 매개변수를 설정하지 않으면 가장 최신의 메시지를 가져온다.
        - 이렇게 생성한 kafkaStream 객체를 앞서 사용한 fileStream과 거의 동일한 방식으로 사용할 수 있다. 유일한 차이점은 DStream 요소의 타입으로 fileStream이 문자열 요소로 구성된 반면 kafkaStream의 각 요소는 두 문자열(키와 메시지)로 구성된 튜플이다.
    - 카프카로 메시지 전송: 이전에서는 지표 계산 결과를 담은 finalStream DStream을 파일에 저장했다. 이 절에서는 파일 대신 카프카 토픽으로 결과를 전송한다. 스파크 스트리밍에서 카프카로 메시지를 전송하는 기능은 DStream의 `foreachRDD` 메서드로 구현할 수 있다. 이 메서드는 임의의 함수를 DStream의 각 RDD 별로 실행한다. 
    카프카에 메시지를 전송하려면 카프카의 Producer객체를 사용해야 한다. Producer 객체는 카프카 브로커에 접속하고, KeyedMessage 객체의 형태로 구성한 메시지를 카프카토픽으로 전송한다. 카프카의 ProducerConfig 객체로 Producer를 설정하고 사용할 수 있다.
    → 직렬화가 불가능한 Producer 객체를 사용할때는 다음과 같이 KafkaProducerWrapper 클래스를 정의하고 이 클래스의 동반 객체로 싱글톤 객체를 생성할 수 있다.

        ```python
        from kafka import KafkaProducer
        class KafkaProducerWrapper(object):
          producer = None
          @staticmethod
          def getProducer(brokerList):
            if KafkaProducerWrapper.producer != None:
              return KafkaProducerWrapper.producer
            else:
              KafkaProducerWrapper.producer = KafkaProducer(bootstrap_servers=brokerList, key_serializer=str.encode, value_serializer=str.encode)
              return KafkaProducerWrapper.producer
        ```

        (2장에서 설명했듯이) 동반 객체를 동일한 이름의 클래스를 선언한 파일 내에 나란히 선언해야 한다. 우리는 스파크 셸이 이 객체를 드라이버에서 초기화하고 직렬화하지 않도록 KafkaProducerWrapper 클래스를 JAR 파일로 컴파일해 사용한다. 

        ```bash
        $ spark-shell --master local[4] \
        --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,\
        org.apache.kafka:kafka_2.11:0.8.2.1
        --jars first-edirion/ch06/kafkaProducerWrapper.jar

        #파이썬에서는 두 라이브러리 중 첫 번째만 전달해도 된다.
        $ spark-shell --master local[4] \
        --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 \
        --jars first-edirion/ch06/kafkaProducerWrapper.jar
        #그 대신 kafka-python 패키지를 설치해야 한다.
        pip install kafka-python
        ```

    - 예제 실행
        - 셸1) orders.txt 파일의 각 줄을 0.1초마다 카프카의 orders 토픽으로 전송하는 streamOrders.sh 스크립트를 실행한다. (`chmod +x streamOrders.sh`하고 실행하기/kafka의 bin 디렉터리를 사용자의 path에 추가해야 한다. )

            ```bash
            $ ./streamOrders.sh 192.168.10.2:9092
            ```

        - 셸2) 스트리밍 프로그램의 결과를 확인한다. 다음의 코드로 metrics 토픽으로 유입된 메시지를 볼 수 있다.

            ```bash
            $ kafka-console-consumer.sh --zookeeper localhost:2181 --topic metrics
            ```

        - 전체 코드

            ```python
            from pyspark.streaming import StreamingContext
            from pyspark.streaming.kafka import KafkaUtils
            from kafka import KafkaProducer
            from kafka.errors import KafkaError

            ssc = StreamingContext(sc, 5)

            kafkaReceiverParams = {"metadata.broker.list": "192.168.10.2:9092"}
            kafkaStream = KafkaUtils.createDirectStream(ssc, ["orders"], kafkaReceiverParams)

            from datetime import datetime
            def parseOrder(line):
              s = line[1].split(",")
              try:
                  if s[6] != "B" and s[6] != "S":
                    raise Exception('Wrong format')
                  return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],
                  "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]
              except Exception as err:
                  print("Wrong line format (%s): " % line)
                  return []

            orders = kafkaStream.flatMap(parseOrder)
            from operator import add
            numPerType = orders.map(lambda o: (o['buy'], 1L)).reduceByKey(add)

            amountPerClient = orders.map(lambda o: (o['clientId'], o['amount']*o['price']))

            amountState = amountPerClient.updateStateByKey(lambda vals, totalOpt: sum(vals)+totalOpt if totalOpt != None else sum(vals))
            top5clients = amountState.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))

            buySellList = numPerType.map(lambda t: ("BUYS", [str(t[1])]) if t[0] else ("SELLS", [str(t[1])]) )
            top5clList = top5clients.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5CLIENTS", arr))

            stocksPerWindow = orders.map(lambda x: (x['symbol'], x['amount'])).reduceByKeyAndWindow(add, None, 60*60)
            stocksSorted = stocksPerWindow.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))
            topStocks = stocksSorted.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5STOCKS", arr))

            finalStream = buySellList.union(top5clList).union(topStocks)
            finalStream.foreachRDD(lambda rdd: rdd.foreach(print))

            def sendMetrics(itr):
              prod = KafkaProducerWrapper.getProducer(["192.168.10.2:9092"])
              for m in itr:
                prod.send("metrics", key=m[0], value=m[0]+","+str(m[1]))
              prod.flush()

            finalStream.foreachRDD(lambda rdd: rdd.foreachPartition(sendMetrics))

            sc.setCheckpointDir("/home/spark/checkpoint/")

            ssc.start()
            ```
    </details>
    <details close>
      <summary>6.3. Performance of Spark Streaming jobs </summary>
      
일반적으로 스트리밍 애플리케이션은 다음과 같은 요구사항을 갖춰야 한다.

1. 각 입력 레코드를 최대한 빨리 처리한다.(낮은 지연 시간)
2. 실시간 데이터의 유량 증가에 뒤처지지 않는다(확장성)
3. 일부 노드에 장애가 발생해도 유실 없이 계속 데이터를 입수한다(장애 내성)

- 미니배치 주기 결정: 스파크 웹 UI streaming 페이지에서 (1) **input rate**(유입 속도-초당 유입된 레코드 개수를 보여준다),  (2) **scheduling delay**(스케줄링 지연 시간-새로운 미니배치의 잡을 스케줄링할 때까지 걸린 시간을 보여준다), (3) **processing time**(처리 시간-각 미니배치의 잡을 처리하는데 걸린 시간을 보여준다), (4) **total delay**(총 지연 시간-각 미니배치를 처리하는 데 소요된 총 시간을 보여준다. )을 확인할 수 있다.

    <img width="502" alt="Screen Shot 2021-07-29 at 5 56 16 PM" src="https://user-images.githubusercontent.com/43725183/127463059-f32bfb1b-a336-4b94-a443-81eab6e35184.png">

    → 미니배치당 총 처리시간(즉, 총 지연 시간)은 미니배치 주기보다 짧아야 하며, 일정한 값으로 유지해야한다. 반면 총 처리 시간이 계속 증가하면 스트리밍 연산을 장기적으로 지속할 수 없다. 이 문제를 해결하려면 (1) **처리 시간을 단축**하거나 (2) **병렬화를 확대**하거나 (3) **유입속도를 제한**해야 한다.

    - 처리시간 단축: 스케줄링이 지연되면 가장 먼저 스트리밍 프로그램의 최적화를 시도해 배치당 처리 시간을 최대한 단축해야한다. 또는 미니배치 주기를 더 길게 늘릴 수도 있다. 또는 클러스터 리소스를 추가로 투입해 처리 시간을 단축할 수도 있다.
    - 병렬화 확대: 모든 CPU 코어를 효율적으로 활용하고 처리량을 늘리려면 결국 병렬화를 확대해야 한다.
    - 유입속도 제한: 더 이상 처리 시간을 줄이거나 병렬화를 확대할 수 없음에도 여전히 스케줄링 지연 시간이 증가한다면, 마지막 처방은 데이터가 유입되는 속도를 제한하는 것이다.
- 장애 내성
    - 실행자의 장애 복구: 데이터가 실행자 프로세스에서 구동되는 리시버로 유입되면 스파크는 이 데이터를 클러스터에 중복 저장한다. 따라서 리시버의 실행자에 장애가 발생해도 다른 노드에서 실행자를 재시작하고 유실된 데이터를 복구할 수 있다. 스파크는 이러한 과정을 자동으로 처리하므로 사용자가 특별히 할 일은 없다.
    - 드라이버의 장애 복구: 드라이버 프로세스가 실패하면 실행자 연결이 끊어지므로 애플리케이션 자체를 재시작해야 한다. 드라이버 프로세스를 재시작하면 스파크 스트리밍은 체크 포인트에 저장된 스트리밍 컨텍스트의 상태를 읽어들여 스트리밍 애플리케이션의 마지막 상태를 복구한다.
    </details>
    <details close>
      <summary>6.4. Structured Streaming </summary>

정형 스트리밍(structured streaming)의 핵심은 스트리밍 연산의 장애 내성과 일관성을 갖추는 데 필요한 세부 사항을 숨겨서 스트리밍 API를 마치 일괄 처리 API처럼 사용할 수 있게 하는 것이다. → 스파크 버전 2.0부터 도입됨.

- **스트리밍 DataFrame 생성**: 스트리밍 DataFrame은 readStream을 호출해 생성한다. 이 메서드는 DataStreamReader를 반환하며 이는 연속으로 유입되는 스트림 데이터를 읽어들인다.

    ```python
    structStream = spark.readStream.text("ch06input")
    structStream.isStreaming()  #True
    structStream.explain()  #실행 계획 확인
    #structStream은 지정된 입력 폴더를 모니터링하고 폴더 아래에 새로 생성된 파일들을 주기적으로 처리한다. 
    ```

- **스트리밍 데이터 출력**: `writeStream` 메서드를 사용해 스트리밍 연산을 시작한다. 여기에 빌더 패턴을 사용하여 DataStreamWriter를 설정할 수 있다. (즉, 다음 설정 함수들을 계속 이어붙일 수 있다.) 
→ trigger, format, outputMode, option, foreach, queryName, append, complete
→ 필요한 모든 설정을 지정한 후 마지막으로 start() 메서드를 호출해 스트리밍 계산을 시작할 수 있다.
- **스트리밍 실행 관리**: start 메서드는 스트리밍 실행의 핸들과 같은 역할을 하는 StreamingQuery 객체를 반한한다. 이 객체로 스트리밍 실행을 관리할 수 있다.
- **정형 스트리밍의 미래**
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 7</b> getting smart with MLlib</summary><br>
  <blockquote>
    <details close>
    <summary>7.1 Introduction to machine learning</summary><br>
      
- 일반적은 머신러닝 프로젝트는 다음 단계로 진행된다.
    1. **데이터 수집**
    2. **데이터 정제 및 준비**: 머신러닝에 적합한 정형 포맷 외에도 다양한 비정형 포맷(텍스트, 이미지, 음성, 이진 데이터 등)으로 저장된 데이터가 필요할 때도 많다. 따라서 이러한 비정형 데이터를 수치형 특성 변수로 변환할 방법을 고안하고 적용해야 한다. 또 결측 데이터를 처리하고, 동일한 정보가 갖가지 다른 값으로 기록되어 있다면(예를 들어 VW와 폭스바겐은 같은 제조사를 의미) 이를 적절히 보완해야 한다. 마지막으로 특징 변수에 스케일링(scailing)을 적용해 데이터의 모든 차원을 비교 가능한 범위로 변환하기도 한다.
    3. **데이터 분석 및 특징 변수 추출**: 데이터 내 상관관계를 조사하고, 필요하다면 데이터를 시각화 한다(or 차원 축소). 다음으로 가장 적절한 머신러닝 알고리즘을 선택하고, 데이터를 훈련 데이터셋과 검증 데이터셋으로 나눈다. 또는 교차 검증(cross-validation) 기법을 활용할 수도 있다. 교차 검증은 데이터셋을 여러 훈련 데이터셋가 검증 데이터셋ㅇ로 계속 나누어 테스트하고 결과들의 평균을 계산한다. 
    4. **모델 훈련**: 입력 데이터를 기반으로 머신 러닝의 학습 알고리즘을 실행해 알고리즘의 매개변수들을 학습하도록 모델을 훈련시킨다. 
    5. **모델 평가**: 모델을 검증 데이터셋에 적용하고 몇 가지 기준으로 모델 성능을 평가한다. 모델 성능이 만족스럽지 못할 때는 더 많은 입력 데이터가 필요하거나 특징 변수를 추출한 방법을 바꾸어야 할 수도 있다. 또는 feature space를 변경하거나 다른 모델을 시도할 수 있다. 어떤 변경을 시도하든 1단계나 2단계로 돌아가야한다. 
    6. **모델 적용**: 마침내 완성된 모델을 웹 사이트의 운영 환경에 배포한다. 
- 머신러닝 알고리즘의 유형
    1. **지도학습(supervised learning)**: 레이블(label)이 포함된 데이터셋을 사용. 회귀와 분류.
    2. **비지도학습(unsupervised learning)**: 레이블이 주어지지 않음. 군집화
- 스파크를 활용한 머신러닝의 장점: (1)분산 처리, (2) 단일 API로 1~6번까지 처리 가능
    </details>
    <details close>
    <summary>7.2. Linear algebra in Spark</summary><br>
      
스파크는 (1) 로컬환경(분산 저장되지 않는 데이터)에서 사용할 수 있는 벡터 및 행렬 클래스와 (2) 분산 환경에서 사용할 수 있는 여러 행렬 클래스를 제공한다. 스파크의 분산 행렬을 사용하면 대규모 데이터의 선형 대수 연산을 여러 머신으로 분산해 처리할 수 있다. 

> 스파크는 희소벡터, 밀집 벡터, 희소 행렬, 밀집 행렬을 모두 지원한다. 희소벡터(또는 행렬)는 원소의 대다수가 0인 벡터(또는 행렬)을 의미한다. 이러한 데이터는 0이 아닌 원소의 위치와 값의 쌍으로 표현하는 것이 더 효율적이다. map 또는 파이썬 딕셔너리와 유사하다. 
반면 밀집벡터(또는 행렬)은 모든 데이터를 저장한다. 다시 말해 배열이나 리스트처럼 원소 위치를 따로 저장하지 않고, 모든 위치의 원소 값을 순차적으로 저장한다. 

1. 로컬 벡터와 로컬 행렬
    - 로컬 벡터 생성

        ```python
        from pyspark.mllib.linalg import Vectors, Vector

        #dense vector
        dv1 = Vectors.dense(5.0,6.0,7.0,8.0)
        dv2 = Vectors.dense([5.0,6.0,7.0,8.0])

        #sparse vector: 벡터 크기, 위치 배열, 값 배열을 인수로 전달
        sv = Vectors.sparse(4, [0,1,2,3], [5.0,6.0,7.0,8.0])

        dv2[2]   #특정 위치의 원소 가져오기 -> 7
        dv1.size   #벡터 크기 조회  -> 4
        dv2.toArray()   #numpy array로 전환
        ```
        <img width="589" alt="Screen Shot 2021-06-22 at 7 41 22 PM" src="https://user-images.githubusercontent.com/43725183/122911029-d445e080-d391-11eb-8621-728616ae3d20.png">



    - 로컬 벡터의 선형 대수 연산

        ```python
        dv1 + dv2   #DenseVector([10.0, 12.0, 14.0, 16.0])
        dv1.dot(dv2)  #174.0
        ```

    - 로컬 밀집 행렬, 로컬 희소 행렬 생성

        ```python
        from pyspark.mllib.linalg import Matrices

        #dense matrix: 행, 열, 데이터 -> columnwise하게 들어간다. 
        dm = Matrices.dense(2,3,[5.0,0.0,0.0,3.0,1.0,4.0])

        #sparse matrix: 원소 값을 CSC(compressed sparse column)포맷으로 전달
        #CRC(https://rfriend.tistory.com/551)
        sm = Matrices.sparse(2,3,[0,1,2,4], [0,1,0,1], [5.0,3.0,1.0,4.0])
        sm.toDense()
        dm.toSparse()
        dm[1,1]
        ```

2. **분산 행렬**: 분산 행렬은 여러 머신에 걸쳐 저장할 수 있고 대량의 행과 열로 구성할 수 있다. 
    - **RowMatrix**: 각 행을 vector 객체에 저장해 RDD를 구성한다.
    - **IndexedRowMatrix**: 행의 원소들을 담은 vector 객체와 이 행의 행렬 내 위치를 저장한다. 아래는 rm이라는 RowMatrix를 IndexedRowMatrix로 변환하는 방법이다.

        ```python
        from pyspark.mllib.linalg.distributed import IndexedRowMatrix, IndexedRow
        rmind = IndexedRowMatrix(rm.rows().zipWithIndex().map(lambda x: IndexedRow(x[1], x[0])))
        ```

    - **CoordinateMatrix**: 개별 원소 값과 해당 원소의 행렬 내 위치(i,j)를 저장한다. 하지만 이 방법으로는 데이터를 효율적으로 저장할 수 없으므로 CoordinateMatrix는 오직 희소 행렬을 저장할 때만 사용해야 한다.
    - **BlockMatrix**: 분산 행렬 간 덧셈 및 곱셈 연산을 지원한다. 행렬을 블록 여러개로 나누고, 각 블록의 원소들을 로컬 행렬의 형태로 저장한 후 이 블록의 전체 행렬 내 위치와 로컬 행렬 객체를 튜플로 구성한다. → ((i, j), Matrix)
    - **분산 행렬의 선형 대수 연산**: 스파크는 분산 행렬의 선형 대수 연산을 제한적으로 제공하므로 나머지 연산은 따로 구현해야 한다.
    </details>
    <details close>
    <summary>7.3. Linear regression</summary><br>
      
선형 회귀는 독립 변수와 목표 변수 사이에 **선형 관계**가 있다고 가정한다. 

- **단순 선형 회귀 (simple linear regression)**
    - model: <a href="https://www.codecogs.com/eqnedit.php?latex=h(x)&space;=&space;w_0&space;&plus;&space;w_1x" target="_blank"><img src="https://latex.codecogs.com/gif.latex?h(x)&space;=&space;w_0&space;&plus;&space;w_1x" title="h(x) = w_0 + w_1x" /></a><br>
    - loss(cost) function(=MSE): <a href="https://www.codecogs.com/eqnedit.php?latex=C(w_0,&space;w_1)&space;=&space;\frac{1}{m}\Sigma(h(x_i)-y_i)^2&space;=&space;\frac{1}{m}\Sigma(w_0&space;&plus;&space;w_1x&space;-y_i)^2" target="_blank"><img src="https://latex.codecogs.com/gif.latex?C(w_0,&space;w_1)&space;=&space;\frac{1}{m}\Sigma(h(x_i)-y_i)^2&space;=&space;\frac{1}{m}\Sigma(w_0&space;&plus;&space;w_1x&space;-y_i)^2" title="C(w_0, w_1) = \frac{1}{m}\Sigma(h(x_i)-y_i)^2 = \frac{1}{m}\Sigma(w_0 + w_1x -y_i)^2" /></a><br>
    - loss function이 최소값을 갖는 <a href="https://www.codecogs.com/eqnedit.php?latex=w_0,&space;w_1" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_0,&space;w_1" title="w_0, w_1" /></a>을 찾는다. → **최적화 과정**
- **다중 선형 회귀 (multiple linear regression)**
    - model: <a href="https://www.codecogs.com/eqnedit.php?latex=h(X)&space;=&space;w_0&space;&plus;&space;w_1x_1&space;&plus;&space;w_2x_2&space;&plus;...&plus;&space;w_nx_n&space;=&space;W^TX" target="_blank"><img src="https://latex.codecogs.com/gif.latex?h(X)&space;=&space;w_0&space;&plus;&space;w_1x_1&space;&plus;&space;w_2x_2&space;&plus;...&plus;&space;w_nx_n&space;=&space;W^TX" title="h(X) = w_0 + w_1x_1 + w_2x_2 +...+ w_nx_n = W^TX" /></a> → 여기서 <a href="https://www.codecogs.com/eqnedit.php?latex=X^T&space;=&space;[1&space;\&space;x_1&space;\&space;...&space;\&space;x_n]" target="_blank"><img src="https://latex.codecogs.com/gif.latex?X^T&space;=&space;[1&space;\&space;x_1&space;\&space;...&space;\&space;x_n]" title="X^T = [1 \ x_1 \ ... \ x_n]" /></a>(즉, 상수 1 추가)
    - loss function: <a href="https://www.codecogs.com/eqnedit.php?latex=C(W)&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2" target="_blank"><img src="https://latex.codecogs.com/gif.latex?C(W)&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2" title="C(W) = \frac{1}{m}\Sigma((W^TX)_i-y_i)^2" /></a>
    - loss function이 최소값을 갖는 W을 찾는다. → **최적화 과정**
        - **정규방정식**으로 찾기: C(W)를 편미분한 값이 0이 되는 W를 계산 
        → <a href="https://www.codecogs.com/eqnedit.php?latex=W&space;=&space;(X^TX)^{-1}X^TY" target="_blank"><img src="https://latex.codecogs.com/gif.latex?W&space;=&space;(X^TX)^{-1}X^TY" title="W = (X^TX)^{-1}X^TY" /></a>
        - 경사 하강법으로 찾기: 정규 방정식의 해를 찾는 작업은 매우 많은 계산량이 필요하다. 특히 데이터셋의 차원이 크고 행이 많을수록 정규 방정식은 사용하기 어렵다. 따라서 정규 방정식보다는 **경사 하강법(gradient-descent method)** 를 더 널리 사용한다.
            1. <a href="https://www.codecogs.com/eqnedit.php?latex=w_0,&space;w_1,&space;...,&space;w_n" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_0,&space;w_1,&space;...,&space;w_n" title="w_0, w_1, ..., w_n" /></a>에 임의의 값을 할당
            2. <a href="https://www.codecogs.com/eqnedit.php?latex=w_j" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_j" title="w_j" /></a>별로 loss function의 편도함수를 계산. → <a href="https://www.codecogs.com/eqnedit.php?latex=\frac{\delta}{\delta&space;w_j}C(W)" target="_blank"><img src="https://latex.codecogs.com/gif.latex?\frac{\delta}{\delta&space;w_j}C(W)" title="\frac{\delta}{\delta w_j}C(W)" /></a>
            3. <a href="https://www.codecogs.com/eqnedit.php?latex=w_j" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_j" title="w_j" /></a>값을 갱신한다. → <a href="https://www.codecogs.com/eqnedit.php?latex=w_j&space;:=&space;w_j&space;-&space;\gamma\frac{\delta}{\delta&space;w_j}C(W)" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_j&space;:=&space;w_j&space;-&space;\gamma\frac{\delta}{\delta&space;w_j}C(W)" title="w_j := w_j - \gamma\frac{\delta}{\delta w_j}C(W)" /></a>, for every j
            (편미분 값이 음수 ↔ <a href="https://www.codecogs.com/eqnedit.php?latex=w_j" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_j" title="w_j" /></a>가 증가하게 갱신됨 ↔ loss function 값이 작아지는 방향)
            (<a href="https://www.codecogs.com/eqnedit.php?latex=\gamma" target="_blank"><img src="https://latex.codecogs.com/gif.latex?\gamma" title="\gamma" /></a>는 gradient descent의 stability를 높일 수 있는 step-size 매개변수이다. )
            4. 2, 3 과정을 반복하다가 loss function 값이 일정 tolerance value보다 작으면 알고리즘이 수렴(즉, 비용함수의 결과 값이 안정적일 때까지)했다고 판단하고 반복을 멈춤.
    </details>
    <details close>
    <summary>7.4. Analyzing and preparing the data</summary>

1. 데이터 불러오기: 파티션을 6개로 함 → RowMatrix로 변경.

    ```python
    from pyspark.mllib.linalg import Vectors, Vector
    housingLines = sc.textFile("ch07/housing.data", 6)
    housingVals = housingLines.map(lambda x: Vectors.dense([float(v.strip()) for v in x.split(",")]))

    from pyspark.mllib.linalg.distributed import RowMatrix
    housingMat = RowMatrix(housingVals)
    ```

2. EDA
    - 분포 확인: `Statistics.colStats(housingVals)`에는 각 열별 평균값, 최대값, 최솟값등을 제공한다.

        ```python
        from pyspark.mllib.stat._statistics import Statistics
        housingStats = Statistics.colStats(housingVals)
        housingStats.min()
        ```

    - 열 코사인 유사도(column cosine similarity) 분석: 두 열을 벡터 두 개로 간주하고 이들 사이의 각도를 구한 값이다.

        ```python
        housingColSims = housingMat.columnSimilarities()
        housingColSims.entries.foreach(lambda x: print(x))
        ```

        upper-triangular matrix가 반환되고, 열 유사도 값은 -1~ 1값을 가진다. 유사도 값이 -1이면 두 열의 방향이 완전히 반대고, 0이면 직각을 이룬다는 의미고, 1이면 두 열의 방향이 같다. 

    - 공분산 행렬 계산: `corr`(org.apache.spark.mllib.stat.Statistics)를 사용하면 스피어만 상관계수 또는 피어슨 상관계수도 구할 수 있다.

        ```python
        housingCovar = housingMat.computeCovariance()
        ```

3. preprocessing
    - 레이블 포인트로 변환: `LabeledPoint`는 거의 모든 스파크 머신 러닝 ㅇㄹ고리즘에 사용하는 객체로, 목표 변수 값과 특징 변수 벡터로 구성된다. 현재 데이터 셋에서 목표 변수는 마지막 칼럼에 있다.

        ```python
        from pyspark.mllib.regression import LabeledPoint
        def toLabeledPoint(x):
          a = x.toArray()
          return LabeledPoint(a[-1], Vectors.dense(a[0:-1]))

        housingData = housingVals.map(toLabeledPoint)
        ```

    - 데이터 분할: training 데이터셋은 모델 훈련에 사용하고, validation 데이터셋은 훈련에 사용하지 않은 데이터에서도 모델이 얼마나 잘 작동하는지 확인하는 용도로 사용한다. 일반적으로 8:2의 비율로 분할한다.

        ```python
        sets = housingData.randomSplit([0.8, 0.2])
        housingTrain = sets[0]
        housingValid = sets[1]
        ```

    - 특징 변수 스케일링 및 평균 정규화: feature scaling은 데이터 범위를 비슷한 크기로 조정하는 작업이고, normalization은 평균이 0에 가깝도록 데이터를 옮기는 작업이다. 이 두작업을 한번에 실행하려면 StandardScaler가 필요하다.

        ```python
        from pyspark.mllib.feature import StandardScaler
        scaler = StandardScaler(True, True).fit(housingTrain.map(lambda x: x.features))

        trainLabel = housingTrain.map(lambda x: x.label)
        trainFeatures = housingTrain.map(lambda x: x.features)
        validLabel = housingValid.map(lambda x: x.label)
        validFeatures = housingValid.map(lambda x: x.features)

        trainScaled = trainLabel.zip(scaler.transform(trainFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
        validScaled = validLabel.zip(scaler.transform(validFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
        ```

        StandardScaler 학습 단계에서는 훈련 데이터셋만 사용하도록 한다. 머신러닝 모델이 미리 습득할 수 있는 정보는 훈련 데이터셋에만 한정해야 하므로 표준화 모델을 학습할 때도 훈련 데이터셋만 사용해야 한다.
    </details>
    <details close>
    <summary>7.5. Fitting and using a linear regression model</summary>
      
- model fitting: 머신러닝 알고리즘을 포함한 반복 알고리즘들은 같은 데이터를 여러번 재사용하기 때문에 캐시를 꼭 활용해야한다.

    ```python
    from pyspark.mllib.regression import LinearRegressionWithSGD
    alg = LinearRegressionWithSGD()
    trainScaled.cache()
    validScaled.cache()
    model = alg.train(trainScaled, iterations=200, intercept=True)
    ```

- prediction: `validPredicts`에는 예측값과 실제 Label이 함께 저장된다.

    ```python
    validPredicts = validScaled.map(lambda x: (float(model.predict(x.features)), x.label))
    validPredicts.collect()

    import math
    RMSE = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
    #4.292515313462585
    ```

    Root Mean Squared Error(RMSE)를 계산하면 4.29이다. 

- model evaluation: RMSE외에도 MAE, R square(모델이 예측하는 목표 변수의 변화량을 설명하는 정도와 설명하지 못하는 정도를 나타내는 척도. 이 값이 1에 가깝다면 모델이 목표 변수가 가진 분산의 많은 부분을 설명할 수 있다는 의미), explainedVariance(R square와 유사한 지표)등을 산출할 수 있다.

    ```python
    from pyspark.mllib.evaluation import RegressionMetrics
    validMetrics = RegressionMetrics(validPredicts)
    validMetrics.rootMeanSquaredError  #4.292515313462585
    validMetrics.meanSquaredError  #18.42568771631079
    validMetrics.r2   #0.7632157714756399
    ```

- 모델 매개변수 해석: coefficient 확인. 특정 가중치(weight, coefficient)값이 0에 가깝다면 해당 차원은 목표 변수에 크게 기여하지 못한다는 의미(스케일링은 진행한 경우에)

    ```python
    import operator
    print("\n".join([str(s) for s in sorted(enumerate([abs(x) for x in model.weights.toArray()]), key=operator.itemgetter(0))]))
    #(0, 1.000538921034109)
    #(1, 1.1273180677794499)
    #(2, 0.24809539149291654)
    #(3, 0.7864653560485336)
    #(4, 1.846301136999396)
    #(5, 2.498899558304125)
    #(6, 0.20040482761985576)
    #(7, 3.3293179912270845)
    #(8, 1.9227159026529925)
    #(9, 0.8581928817743215)
    #(10, 1.9938882718949151)
    #(11, 1.0187427812723926)
    #(12, 3.9936868478891263)
    ```

    12번(LSTAT, 저소득층 인구 비율)컬럼과 7번(DIS, 보스턴 고용 센터 다섯 군데까지의 가중 거리)컬럼의 영향력이 큰 것으로 나타났다. 

- 모델의 저장 및 불러오기: 스파크는 학습된 모델을 Parquet 파일 포맷으로 파일 시스템에 저장하고, 추후 다시 로드할 수 있는 방법을 제공한다. 스파크는 지정된 경로에 새로운 디렉터리를 생성하고 모델의 **데이터 파일**(모델의 weight와 intercept을 저장)과 **메타데이터 파일**(모델을 구현한 클래스 이름, 모델 파일의 버전, 모델에 사용된 특징 변수의 개수)을 Parquet 포맷으로 저장한다.

    ```python
    #저장하기
    model.save(sc, "ch07/ch07output/model")

    #다시 모델 불러오기
    from pyspark.mllib.regression import LinearRegressionModel
    model = LinearRegressionModel.load(sc, "ch07/ch07output/model")
    ```
    </details>
    <details close>
    <summary>7.6. Tweaking the algorithm</summary>
      
1. 적절한 **감마 값(이동거리)** 와 **반복 횟수(iteration)** 을 찾는 방법
    - 감마값이 너무 작으면 경사를 내려가는 거리가 짧으므로 알고리즘이 수렴하려면 더 많은 반복 단계를 거쳐야 한다. 반대로 너무 크면 알고리즘이 수렴하지 않을 수 있다(아래예시에서 3인 경우). 적절한 이동거리는 데이터셋에 따라 다르다.
    - iteration 수가 너무 크면 알고리즘 학습시간이 너무 오래 걸리고, 너무 적으면 최저점에 도달하지 못할 수 있다(→ 아래 예시에서 600번 반복을 한다고 해서 더 나은 결과를 보여주지는 않는다).
    - 이동거리와 반복 횟수의 최적 값을 찾는 방법 중 하나는 값의 여러 조합을 하나씩 실험해보고 가장 좋은 결과를 내는 조합을 고르는 것이다.

        ```python
        def iterateLRwSGD(iterNums, stepSizes, train, valid):
          from pyspark.mllib.regression import LinearRegressionWithSGD
          import math
          for numIter in iterNums:
            for step in stepSizes:
              alg = LinearRegressionWithSGD()
              model = alg.train(train, iterations=numIter, step=step, intercept=True)
              rescaledPredicts = train.map(lambda x: (float(model.predict(x.features)), x.label))
              validPredicts = valid.map(lambda x: (float(model.predict(x.features)), x.label))
              meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              print("%d, %5.3f -> %.4f, %.4f" % (numIter, step, meanSquared, meanSquaredValid))
              #Uncomment if you wish to see weghts and intercept values:
              #print("%d, %4.2f -> %.4f, %.4f (%s, %f)" % (numIter, step, meanSquared, meanSquaredValid, model.weights, model.intercept))

        iterateLRwSGD([200, 400, 600], [0.05, 0.1, 0.5, 1, 1.5, 2, 3], trainScaled, validScaled)
        # 200, 0.050 -> 7.5420, 7.4786
        # 200, 0.100 -> 5.0437, 5.0910
        # 200, 0.500 -> 4.6920, 4.7814
        # 200, 1.000 -> 4.6777, 4.7756
        # 200, 1.500 -> 4.6751, 4.7761
        # 200, 2.000 -> 4.6746, 4.7771
        # 200, 3.000 -> 108738480856.3940, 122956877593.1419
        # 400, 0.050 -> 5.8161, 5.8254
        # 400, 0.100 -> 4.8069, 4.8689
        # 400, 0.500 -> 4.6826, 4.7772
        # 400, 1.000 -> 4.6753, 4.7760
        # 400, 1.500 -> 4.6746, 4.7774
        # 400, 2.000 -> 4.6745, 4.7780
        # 400, 3.000 -> 25240554554.3096, 30621674955.1730
        # 600, 0.050 -> 5.2510, 5.2877
        # 600, 0.100 -> 4.7667, 4.8332
        # 600, 0.500 -> 4.6792, 4.7759
        # 600, 1.000 -> 4.6748, 4.7767
        # 600, 1.500 -> 4.6745, 4.7779
        # 600, 2.000 -> 4.6745, 4.7783
        # 600, 3.000 -> 4977766834.6285, 6036973314.0450
        ```

        → 대부분 검증 데이터셋의 RMSE는 훈련 RMSE보다 크다 (overfitting)

2. **고차 다항식 추가**: 스파크는 고차 다항식을 사용한 비선형 회귀모델을 지원하지 않는다. 그 대신 기존 변수의 제곱값을 추가해 데이터셋을 확장해서 비슷한 효과를 낼 수 있다. 또한 비슷하게 두 변수가 공동으로 영향을 줄 때는 $x_1*x_2$와 같은 상호작용항(interaction term)을 추가하는 것도 도움이 된다. → 위의 예시에서 고차 다항식을 추가했을때 RMSE자체는 감소하였다. (but, RMSE가 감소했다고 무조건 좋은 것인가?)

    ```python
    def addHighPols(v):
      import itertools
      a = [[x, x*x] for x in v.toArray()]
      return Vectors.dense(list(itertools.chain(*a)))

    housingHP = housingData.map(lambda v: LabeledPoint(v.label, addHighPols(v.features)))
    len(housingHP.first().features)  #26

    setsHP = housingHP.randomSplit([0.8, 0.2])
    housingHPTrain = setsHP[0]
    housingHPValid = setsHP[1]
    scalerHP = StandardScaler(True, True).fit(housingHPTrain.map(lambda x: x.features))
    trainHPLabel = housingHPTrain.map(lambda x: x.label)
    trainHPFeatures = housingHPTrain.map(lambda x: x.features)
    validHPLabel = housingHPValid.map(lambda x: x.label)
    validHPFeatures = housingHPValid.map(lambda x: x.features)
    trainHPScaled = trainHPLabel.zip(scalerHP.transform(trainHPFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
    validHPScaled = validHPLabel.zip(scalerHP.transform(validHPFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
    trainHPScaled.cache()
    validHPScaled.cache()

    iterateLRwSGD([200, 400], [0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5], trainHPScaled, validHPScaled)
    #이동거리가 1.1이고 반복횟수가 400회일때 결과가 가장 좋다(# 400, 1.100 -> 4.0378, 3.9836)

    iterateLRwSGD([200, 400, 800, 1000, 3000, 6000], [1.1], trainHPScaled, validHPScaled)
    #이동거리는 고정하고 반복횟수만 늘려보자. 
    # 200, 1.100 -> 4.1605, 4.0108
    # 400, 1.100 -> 4.0378, 3.9836
    # 800, 1.100 -> 3.9438, 3.9901
    # 1000, 1.100 -> 3.9199, 3.9982
    # 3000, 1.100 -> 3.8332, 4.0633
    # 6000, 1.100 -> 3.7915, 4.1138
    #오히려 RMSE가 증가한다.
    ```

3. **편향-분산 상충 관계와 모델의 복잡도(bias-variance tradeoff)**: 머신러닝 모델은 훈련 데이터셋의 데이터를 잘 학습해야 하면서 현재까지 관찰되지 않은 다른 데이터에서도 좋은 성능을 낼 수 있는 확장성 또한 갖추어야 한다. 그러나 두 가지 목표를 완벽하게 이룰 수는 없다. 
    - bias가 크다(모델을 바라보는 관점이 편향되었다. )
     → **underfitting** → 모델을 더 복잡하게 만들어야 한다.
    - variance가 크다(예측값의 변동 폭이 더 크다)
     → **overfitteing** → 모델을 덜 복잡하게 만들어야 한다.
4. 잔차 차트 그리기(**residual plot**): 모델의 복잡도를 어디까지 올려야 할지 잔차 차트를 통해 결정해보자. residual은 훈련 데이터셋의 각 예제별 실제 레이블 값과 모델이 예상한 레이블 값 차이이다. 잔차 차트는 X 축에 예측값을 설정하고 Y 축에 잔차 값을 설정해 그린다.
→ 이상적인 잔차 차트에는 눈에 띄는 패턴이 없어야 한다. X축의 모든 점이 거의 동일한 Y 값을 가져야 하며, 점들을 가로지르는 최적선을 그렸을때 선이 거의 평평해야 한다. 반면 최적선이 U자 같은 모양을 보인다면 이는 일부 차원에서 비선형 모델이 더 적절할 수 있다는 의미다. 
→ 자세한 잔차 차트 내용은 책의 범위를 벗어나므로 설명하지 않는다. 모델 성능에 대한 자세한 정보를 얻으려면 잔차 차트를 연구하는데 많은 노력을 기울여야 한다. 
5. **규제화(regularization)**를 사용해 과적합 방지: 과적합을 방지하는 한 가지 방법으로 모델 매개변수 값이 클수록 불이익을 가해 모델 편향을 키우고 분산을 낮추는 regularization 기법을 사용하자. 

    →  <a href="https://www.codecogs.com/eqnedit.php?latex=C(W)&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2&space;-&space;\beta&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2&space;-\lambda||w||" target="_blank"><img src="https://latex.codecogs.com/gif.latex?C(W)&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2&space;-&space;\beta&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2&space;-\lambda||w||" title="C(W) = \frac{1}{m}\Sigma((W^TX)_i-y_i)^2 - \beta = \frac{1}{m}\Sigma((W^TX)_i-y_i)^2 -\lambda||w||" /></a>

    여기서 람다는 일반화 매개변수이고 여기에 가중치 벡터의 L1 norm(=$||w||_1$ → 벡터요소의 절대값을 합한 값) 또는 L2 norm(=$||w||_2$→ 벡터의 길이)을 곱해준다. 

    - Lasso regression(L1) : 개별 가중치는 0으로 만들어 변수를 데이터셋에서 완전히 제거한다.

        ```python
        def iterateLasso(iterNums, stepSizes, regParam, train, valid):
          from pyspark.mllib.regression import LassoWithSGD
          for numIter in iterNums:
            for step in stepSizes:
              alg = LassoWithSGD()
              model = alg.train(train, intercept=True, iterations=numIter, step=step, regParam=regParam)
              rescaledPredicts = train.map(lambda x: (model.predict(x.features), x.label))
              validPredicts = valid.map(lambda x: (model.predict(x.features), x.label))
              meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              print("%d, %5.3f -> %.4f, %.4f" % (numIter, step, meanSquared, meanSquaredValid))
              #print("\tweights: %s" % model.weights)

        iterateLasso([200, 400, 1000, 3000, 6000, 10000, 15000], [1.1], 0.01, trainHPScaled, validHPScaled)
        #Our results:
        # 200, 1.100 -> 4.1762, 4.0223
        # 400, 1.100 -> 4.0632, 3.9964
        # 1000, 1.100 -> 3.9496, 3.9987
        # 3000, 1.100 -> 3.8636, 4.0362
        # 6000, 1.100 -> 3.8239, 4.0705
        # 10000, 1.100 -> 3.7985, 4.1014
        # 15000, 1.100 -> 3.7806, 4.1304
        ```

    - Ridge regression(L2)

        ```python
        def iterateRidge(iterNums, stepSizes, regParam, train, valid):
          from pyspark.mllib.regression import RidgeRegressionWithSGD
          import math
          for numIter in iterNums:
            for step in stepSizes:
              alg = RidgeRegressionWithSGD()
              model = alg.train(train, intercept=True, regParam=regParam, iterations=numIter, step=step)
              rescaledPredicts = train.map(lambda x: (model.predict(x.features), x.label))
              validPredicts = valid.map(lambda x: (model.predict(x.features), x.label))
              meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              print("%d, %5.3f -> %.4f, %.4f" % (numIter, step, meanSquared, meanSquaredValid))

        iterateRidge([200, 400, 1000, 3000, 6000, 10000], [1.1], 0.01, trainHPScaled, validHPScaled)
        # Our results:
        # 200, 1.100 -> 4.2354, 4.0095
        # 400, 1.100 -> 4.1355, 3.9790
        # 1000, 1.100 -> 4.0425, 3.9661
        # 3000, 1.100 -> 3.9842, 3.9695
        # 6000, 1.100 -> 3.9674, 3.9728
        # 10000, 1.100 -> 3.9607, 3.9745
        ```

    → 일반화 기법은 모델 과적합 현상을 완화할 수 있다. 일반화 매개변수($\beta$)가 증가할수록 모델 과적합은 감소한다. 또, 일반화 기법은 모델 성능에 크게 기여하지 못하는 차원들의 영향력을 감소시킬 수 있으므로, 데이터셋의 차원이 매우 많은 상황에서도 오차를 더 빠르게 최적화할 수 있다. 그러나 일반화 매개변수의 최적 값 또한 찾아야 하기 때문에 모델 훈련과정을 더 복잡하게 만든다는 단점은 있다. 

6. **k-겹 교차 검증(k-fold cross-validation)**: 전체 데이터셋을 동일한 크기의 k개 부분 집합으로 나눈다. 그런 다음 각 부분 집합을 전체 데이터셋에서 제외해 총 k개의 훈련 데이터셋을 만들고, 이를 사용해 모델 k개를 훈련시킨다. 각 모델의 훈련 데이터셋에서 제외된 부분 집합은 해당 모델의 검증 데이터셋으로 사용하며, 나머지 부분 집합은 훈련 데이터셋으로 사용한다. 
→ 먼저 변수 값의 각 조합별로 모델 k개를 모두 학습시키고, 이 모델들이 기록한 **오차 평균을 계산**한 후 마지막으로 가장 작은 평균 오류를 기록한 변수 값의 조합을 선택하는 것이다.  
→ k-겹 교차 검증이 중요한 이유는 어떤 훈련 데이터셋과 검증 데이터셋을 사용했느냐에 따라 모델 학습 결과가 크게 달라지기 때문이다.
    </details>
    <details close>
    <summary>7.7. Optimizing linear regression</summary>
      
선형 회귀 알고리즘이 loss function의 최저점을 더 빨리 찾을 수 있는 두 가지 방법을 추가로 알아보자.

1. 미니배치 기반 확률적 경사 하강법
    - **Batch Gradient Descent(BGD)**: 먼저 설명한 경사 하강법의 각 단계에서는 전체 데이터셋을 한꺼번에 사용해 오차를 계산하고 가중치 값을 갱신했다. 이러한 방법을 BGD라고 한다.
    - 반면, 미니배치 기반 확률적 경사 하강법은 각 반복 단계에서 데이터셋의 일부분(k개, 여기서 k는 전체 샘플 수보다 작은 수)만 사용한다. k값이 1인 경우(즉, 알고리즘이 각 반복단계에서 오직 예제 한 개만 학습에 사용할 경우), 이를 **확률적 경사 하강법(Stochastic Gradient Descent, SGD)**라고 한다.
    - 미니배치 기반 SGD를 사용하면 알고리즘을 병렬로 학습할때 계산량을 크게 줄일 수 있다. 그러나 모델을 충분히 훈련시키려면 반복 횟수를 늘려야 한다. 미니배치 기반 SGD는 수렴하기 더 어렵지만 최저점에 충분히 가깝게 갈 수 있다. 미니배치 데이터셋의 크기(k)를 작게 설정하면 알고리즘은 더 **확률적으로** 움직인다. 즉, 알고리즘이 비용 함수의 최저점을 향해 **더 무작위로** 움직인다는 의미다. 반면 k값으 크게 설정하면 알고리즘은 더욱 안정적으로 이동한다. 하지만 어느 경우에든 최저점에 충분히 도달하며 BGD와 유사한 결과를 얻을 수 있다.
    - 스파크에서 사용할 경우 miniBatchFraction에 0~1값을 설정한다. 1로 설정하면 BGD와 마찬가지로 각 학습 단계에서 전체 데이터셋을 사용한다. 
    → 반복 회수 매개변수는 전체 데이터셋이 총 100회 정도 반복 사용할 수 있는 값을 대체로 선택한다.

        ```python
        def iterateLRwSGDBatch(iterNums, stepSizes, fractions, train, valid):
          for numIter in iterNums:
            for step in stepSizes:
              for miniBFraction in fractions:
                alg = LinearRegressionWithSGD()
                model = alg.train(train, intercept=True, iterations=numIter, step=step, miniBatchFraction=miniBFraction)
                rescaledPredicts = train.map(lambda x: (model.predict(x.features), x.label))
                validPredicts = valid.map(lambda x: (model.predict(x.features), x.label))
                meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
                meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
                print("%d, %5.3f %5.3f -> %.4f, %.4f" % (numIter, step, miniBFraction, meanSquared, meanSquaredValid))

        iterateLRwSGDBatch([400, 1000], [0.05, 0.09, 0.1, 0.15, 0.2, 0.3, 0.35, 0.4, 0.5, 1], [0.01, 0.1], trainHPScaled, validHPScaled)
        iterateLRwSGDBatch([400, 1000, 2000, 3000, 5000, 10000], [0.4], [0.1, 0.2, 0.4, 0.5, 0.6, 0.8], trainHPScaled, validHPScaled)
        ```

        결론적으로 미니배치 기반 SGD는 BGD와 비슷한 수준이 RSME를 달성할 수 있다. 그렇다면 계산 성능을 개선할 수 있는 미니배치 기반 SGD를 사용하는 편이 낫다. 

2. LBFGS 최적화: LBFGS(Limited-memory BFGS)는 제한된 메모리로 BFGS(Broyden-Fletcher-Goldfarb-Shanno)알고리즘을 추정하는 기법이다. BFGS는 다차원 함수를 최적화하는 알고리즘으로 어떤 함수의 이계도 함수를 표현한 행렬(헤세 행렬, Hessian matrix)를 구하고, 이 헤세 행렬의 역행렬을 근사치로 계산한 후 n X n 행렬을 메모리에 유지한다. 반면 LBFGS는 과거 갱신 값을 최근 열 개 미만으로 유지하므로 차원 개수가 많을 때 메모리를 더 효율적으로 사용한다. 

    → LBFGS는 지금까지 RMSE 결과와 거의 유사한 결과를 보이면서도 계산 성능을 크게 개선할 수 있다.
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 8</b> ML: classification and clustering</summary>
   <blockquote>
    <details close>
      <summary>8.1. Spark ML library </summary>
      
스파크에서 사용할 수 있는 분류 알고리즘에는 로지스틱회귀, 나이브 베이즈, 서포트 벡터 머신, 의사 결정 트리, 랜덤 포레스트가 있다. 스파크 ML의 주요 컴포넌트인 추정자, 변환자, 평가자, ML 매개변수, 파이프라인을 자세히 알아보자.
<img width="540" alt="Screen Shot 2021-06-28 at 10 54 00 PM" src="https://user-images.githubusercontent.com/43725183/123648225-bc73ce00-d863-11eb-888d-6b1238f7f30c.png">


1. **변환자(transformer)**: 한 데이터셋을 다른 데이터셋으로 변환하는 머신 러닝 컴포넌트를 구현할 수 있다. e.g. `transform`
2. **추정자(estimator)**: 주어진 데이터셋을 학습해 변환자(transformer)를 생성한다. e.g. `fit`
3. **평가자(evaluator)**: 모델 성능을 단일 지표로 평가한다. e.g. RMSE
4. ML 매개변수: `Param` 클래스로 매개변수 유형을 정의 → `ParamPair`에는 매개변수 유형(즉, `Param` 객체)과 변수 값의 쌍을 저장 → `ParamMap`은 여러 `ParamPair` 객체를 저장한다. 
(지정방법 1) ParamPair 또는 ParamMap 객체를 추정자의 `fit` 메서드 또는 변환자의 `transform` 메서드에 전달한다. 

    (지정방법 2) 각 매개변수의 set 메서드를 호출한다. e.g. `setRegParam(0.1)`

5. ML 파이프라인(`PipelineModel`): 앞의 객체들을 결합해 파이프라인을 구성할 수 있다.
    </details>
    <details close>
      <summary>8.2. Logistic regression </summary>
      
- 로지스틱 회귀모델: 로지스틱 회귀는 톡정 샘플이 특정 클래스에 속할 확률을 계산한다. 다음 식의 좌변은 logit(log-odds)이라고 한다.
    - model: <a href="https://www.codecogs.com/eqnedit.php?latex=ln(\frac{p}{1-p})&space;=&space;w_0&space;&plus;&space;w_1x_1&space;&plus;&space;...&space;&plus;w_nxz-n&space;=&space;W^TX" target="_blank"><img src="https://latex.codecogs.com/gif.latex?ln(\frac{p}{1-p})&space;=&space;w_0&space;&plus;&space;w_1x_1&space;&plus;&space;...&space;&plus;w_nxz-n&space;=&space;W^TX" title="ln(\frac{p}{1-p}) = w_0 + w_1x_1 + ... +w_nxz-n = W^TX" /></a>  → <a href="https://www.codecogs.com/eqnedit.php?latex=p&space;=&space;\frac{1}{1&plus;e^{-W^TX}}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?p&space;=&space;\frac{1}{1&plus;e^{-W^TX}}" title="p = \frac{1}{1+e^{-W^TX}}" /></a>
    - loss(cost) function(-log-likelihood function): <a href="https://www.codecogs.com/eqnedit.php?latex=l(w)&space;=&space;\Sigma&space;y_iW^TXi&space;-&space;ln(1-e^{W^TXi})" target="_blank"><img src="https://latex.codecogs.com/gif.latex?l(w)&space;=&space;\Sigma&space;y_iW^TXi&space;-&space;ln(1-e^{W^TXi})" title="l(w) = \Sigma y_iW^TXi - ln(1-e^{W^TXi})" /></a>
    - 경사 하강법으로 loss function의 최저값을 찾는다.
- 로지스틱 회귀분석 실습
    1. 데이터 준비
        - DataFrame 만들기

            ```python
            #데이터 불러와서 숫자 자료형 실수로 변경하기
            def toDoubleSafe(v):
                try:
                    return float(v)
                except ValueError:
                    return v

            census_raw = sc.textFile("ch08/adult.raw", 4).map(lambda x:  x.split(", "))
            census_raw = census_raw.map(lambda row:  [toDoubleSafe(x) for x in row])

            #5장의 세번째 방법으로 DataFrame 만들기
            from pyspark.sql.types import *
            columns = ["age", "workclass", "fnlwgt", "education", "marital_status",
                "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss",
                "hours_per_week", "native_country", "income"]
            adultschema = StructType([
                StructField("age",DoubleType(),True),
                StructField("capital_gain",DoubleType(),True),
                StructField("capital_loss",DoubleType(),True),
                StructField("education",StringType(),True),
                StructField("fnlwgt",DoubleType(),True),
                StructField("hours_per_week",DoubleType(),True),
                StructField("income",StringType(),True),
                StructField("marital_status",StringType(),True),
                StructField("native_country",StringType(),True),
                StructField("occupation",StringType(),True),
                StructField("race",StringType(),True),
                StructField("relationship",StringType(),True),
                StructField("sex",StringType(),True),
                StructField("workclass",StringType(),True)
            ])
            from pyspark.sql import Row
            dfraw = sqlContext.createDataFrame(census_raw.map(lambda row: Row(**{x[0]: x[1] for x in zip(columns, row)})), adultschema)
            dfraw.show()
            ```

        - 결측값 다루기

            **결측값을 다루는 4가지 방법**<br>
            (1) 특정 칼럼(특정 변수)의 데이터가 과도하게 누락되어 있다면, 예측 결과에 부정적인 영향을 줄 수 있으므로 이 칼럼의 모든 데이터를 데이터셋에서 제거할 수 있다. <br>
            (2) 개별 예제(row)에 데이터가 누락된 칼럼이 많다면, 이 행을 데이터셋에서 제거할 수 있다.<br>
            (3) 각 칼럼의 결측값을 해당 칼럼의 가장 일반적인 값으로 대체할 수 있다. e.g. mode<br>
            (4) 별도의 분류 또는 회귀 모델을 학습해 결측 값을 예측할 수 있다. <br>

            ```python
            dfraw.groupBy(dfraw["workclass"]).count().foreach(print)
            #Row(workclass='Self-emp-not-inc', count=3862)
            #Row(workclass='Without-pay', count=21)
            #Row(workclass='Never-worked', count=10)
            #Row(workclass='Federal-gov', count=1432)
            #Row(workclass='Self-emp-inc', count=1695)
            #Row(workclass='?', count=2799)
            #Row(workclass='Private', count=33906)
            #Row(workclass='Local-gov', count=3136)
            ```

            workclass라는 컬럼의 결측치(?)는 총 2799개이다. 이 열의 최빈값은 Private이다. `DataFrameNafunctions`를 사용해서 imputation을 진행하자.

            ```python
            dfrawrp = dfraw.na.replace(["?"], ["Private"], ["workclass"])
            dfrawrpl = dfrawrp.na.replace(["?"], ["Prof-specialty"], ["occupation"])
            dfrawnona = dfrawrpl.na.replace(["?"], ["United-States"], ["native_country"])
            ```

        - 범주형 변수 one-hot encoding 해주기
            <img width="579" alt="Screen Shot 2021-07-04 at 3 32 08 PM" src="https://user-images.githubusercontent.com/43725183/124375413-05e97080-dcdd-11eb-8210-7a58d5ca923b.png">

           
            1. StringIndexer 사용: String 타입의 범주 값을 해당 값의 정수 번호로 변환한다. 또한  이 방법은 칼럼을 변환하면서 해당 칼럼의 메타데이터를 추가한다. 

                ```python
                def indexStringColumns(df, cols):
                    from pyspark.ml.feature import StringIndexer
                    #variable newdf will be updated several times
                    newdf = df
                    for c in cols:
                        si = StringIndexer(inputCol=c, outputCol=c+"-num")
                        sm = si.fit(newdf)
                        newdf = sm.transform(newdf).drop(c)
                        newdf = newdf.withColumnRenamed(c+"-num", c)
                    return newdf

                dfnumeric = indexStringColumns(dfrawnona, ["workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "income"])
                ```

            2. OneHotEncoder로 데이터 인코딩

                ```python
                def oneHotEncodeColumns(df, cols):
                    from pyspark.ml.feature import OneHotEncoder
                    newdf = df
                    for c in cols:
                        onehotenc = OneHotEncoder(inputCol=c, outputCol=c+"-onehot", dropLast=False)
                        newdf = onehotenc.transform(newdf).drop(c)
                        newdf = newdf.withColumnRenamed(c+"-onehot", c)
                    return newdf

                dfhot = oneHotEncodeColumns(dfnumeric, ["workclass", "education", "marital_status", "occupation", "relationship", "race", "native_country"])
                ```

            3. VectorAssembler로 데이터 병합: input data인 labeled point 생성. 

                ```python
                from pyspark.ml.feature import VectorAssembler
                va = VectorAssembler(outputCol="features", inputCols=dfhot.columns[0:-1])
                lpoints = va.transform(dfhot).select("features", "income").withColumnRenamed("income", "label")
                ```

    2. 로지스틱 회귀 모델 training
        - 모델 training

            ```python
            #training data, test data 분리
            splits = lpoints.randomSplit([0.8, 0.2])
            adulttrain = splits[0].cache()
            adultvalid = splits[1].cache()

            from pyspark.ml.classification import LogisticRegression
            lr = LogisticRegression(regParam=0.01, maxIter=1000, fitIntercept=True)
            lrmodel = lr.fit(adulttrain)
            # 아래처럼 직접 fit 메서드에 매개변수들을 지정할 수도 있다. 
            lrmodel = lr.setParams(regParam=0.01, maxIter=500, fitIntercept=True).fit(adulttrain)
            ```

        - 가중치(parameter) 해석: 모델의 가중치는 로그 오즈값에 선형적인 영향을 미친다. → 각 가중치에 해당하는 특징 변수 값에 1을 더하고 다른 특징 변수 값은 그대로 유지했을 때 확률 값이 어떻게 달라지는지 살펴본다. 예를 들어 x1에 1을 더하는 것은 곧 오즈에 $e^{w_1}$을 곱하는 것과 같다.

            ```python
            lrmodel.coefficients
            #DenseVector([0.0094, 0.0, 0.0002, 0.0, 0.0125, -0.2029, 6.3247, -0.0172, -0.1176, 0.0225, -0.0678, 0.1919, 0.2416, -0.3281, -0.4936, -0.164, -0.0332, 0.323, 0.5022, 0.0676, -0.355, 0.0996, -0.4365, -0.4936, 0.6826, -0.4989, -0.2283, 0.6962, -0.4803, -0.5721, -0.6033, 0.3848, -0.3234, -0.1194, -0.1483, -0.1483, -0.0953, 0.3268, 0.1322, 0.0133, 0.3251, -0.0601, 0.071, -0.3152, -0.1634, -0.0533, -0.3048, -0.347, 0.1842, 0.1318, -0.4862, 0.0414, 0.2727, -0.0799, -0.3659, -0.1996, 0.5765, -0.309, 0.0696, -0.0875, 0.0495, -0.1177, -0.1746])
            lrmodel.intercept
            #-5.1669954991723195
            ```

    3. 모델 evaluation
        - Confusion matrix
            <img width="584" alt="Screen Shot 2021-07-04 at 3 31 44 PM" src="https://user-images.githubusercontent.com/43725183/124375399-f2d6a080-dcdc-11eb-9bfa-f12156ded39d.png">  

            
            - **precision**(정밀도): TP/(TP + FP)
            - **recall**(재현도): TP/(TP + FN) → 다른 말로 sensitivity(민감도), TPR(True Positive Rate, 진양성률), hit rate(적중률)이라고 한다.
            - **F-measure**(F 점수): precision과 recall의 조화 평균으로 계산한다 (f1 = 2PR/(P + R)
        - **Precision-recall curve(= PR curve)**
            - 확률 임계치(default 0.5)를 높이면 위양성이 줄어들면서 정밀도가 상승한다. 하지만 데이터셋의 실제 양성을 더 적게 식별하므로 재현율이 하락한다. 반대로 임계치를 낮추면 양성(TP, NP)가 많아지므로 정밀도는 떨어지지만 재현율이 상승한다.
            - PR curve 아래 영역의 면적이 바로 **AUPRC**(Area Under Precision-Recall Curve) 이다.
        - **ROC(Receiver Operating Characteristic) curve**
            - TPR(=recall)을 y축에 그리고 FPR을 x 축에 그린다. 여기서 FPR은 FP/(FP + TN)으로 계산한다.
            - 이상적인 모델은 FPR이 낮고(위양성이 적고) TPR이 높아야(위음성이 적어야)하며, 따라서 ROC 곡선도 왼쪽 위 모서리에 가깝게 그려져야 한다. 대각선에 가까운 ROC 곡선은 모델이 거의 무작위에 가까운 결과를 예측한다는 뜻이다.
        - BinaryClassificationEvaluator로 AUC, AUPRC 구하기

            ```python
            validpredicts = lrmodel.transform(adultvalid)

            from pyspark.ml.evaluation import BinaryClassificationEvaluator
            bceval = BinaryClassificationEvaluator()

            #AUC 구하기
            bceval.evaluate(validpredicts)
            bceval.getMetricName()  #areaUnderROC

            #AUPRC 구하기
            bceval.setMetricName("areaUnderPR")
            bceval.evaluate(validpredicts)
            ```

    4. k-fold cross validation
    → 매개변수 값의 여러 조합을 빠르게 비교할 수 있다. 

        ```python
        from pyspark.ml.tuning import CrossValidator
        from pyspark.ml.tuning import ParamGridBuilder

        #k=5로 지정
        cv = CrossValidator().setEstimator(lr).setEvaluator(bceval).setNumFolds(5)

        #매개변수 값의 조합을 생성
        paramGrid = ParamGridBuilder().\
        addGrid(lr.maxIter, [1000]).\
        addGrid(lr.regParam, [0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]).build()

        cv.setEstimatorParamMaps(paramGrid)
        cvmodel = cv.fit(adulttrain)

        #best model의 weight 확인
        cvmodel.bestModel.weights
        BinaryClassificationEvaluator().evaluate(cvmodel.bestModel.transform(adultvalid))
        ```

- 다중 클래스 로지스틱 회귀
    1. 다중 클래스 로지스틱 회귀 방법 → 생략
    2. 이진 분류 모델로 One vs. Rest(OVR) 하기
        - 이 절에서는 handwritten 데이터 셋을 분류하는 예제로 OVR 클래스들의 사용법을 알아본다. 이 데이터는 [http://mng.bz/9jHs에서](http://mng.bz/9jHs에서) 다운로드 받을 수 있으며 0부터 9까지 숫자를 손으로 직접 쓴 샘플 이미지 1만 992개로 구성되어 있다. 각 샘플 이미지에는 밝기 값이 0~100 사이인 픽셀 정보가 16개 저장되어 있다.
        - 파이썬에서는 OneVsRest를 버전 2.0.0부터 사용할 수 있다.

            ```python
            penschema = StructType([
                StructField("pix1",DoubleType(),True),
                StructField("pix2",DoubleType(),True),
                StructField("pix3",DoubleType(),True),
                StructField("pix4",DoubleType(),True),
                StructField("pix5",DoubleType(),True),
                StructField("pix6",DoubleType(),True),
                StructField("pix7",DoubleType(),True),
                StructField("pix8",DoubleType(),True),
                StructField("pix9",DoubleType(),True),
                StructField("pix10",DoubleType(),True),
                StructField("pix11",DoubleType(),True),
                StructField("pix12",DoubleType(),True),
                StructField("pix13",DoubleType(),True),
                StructField("pix14",DoubleType(),True),
                StructField("pix15",DoubleType(),True),
                StructField("pix16",DoubleType(),True),
                StructField("label",DoubleType(),True)
            ])
            pen_raw = sc.textFile("first-edition/ch08/penbased.dat", 4).map(lambda x:  x.split(", ")).map(lambda row: [float(x) for x in row])

            #스파크에 데이터 로드
            dfpen = sqlContext.createDataFrame(pen_raw.map(Row.fromSeq(_)), penschema)
            def parseRow(row):
                d = {("pix"+str(i)): row[i-1] for i in range(1,17)}
                d.update({"label": row[16]})
                return Row(**d)

            dfpen = sqlContext.createDataFrame(pen_raw.map(parseRow), penschema)
            va = VectorAssembler(outputCol="features", inputCols=dfpen.columns[0:-1])
            penlpoints = va.transform(dfpen).select("features", "label")

            #training dataset과 test dataset으로 분리
            pensets = penlpoints.randomSplit([0.8, 0.2])
            pentrain = pensets[0].cache()
            penvalid = pensets[1].cache()

            penlr = LogisticRegression(regParam=0.01)
            ```
    </details>
    <details close>
      <summary>8.3. Decision trees and random forests </summary>
      
1. Decision tree
    - decision tree: 과적합 현상에 빠지기 쉬워 입력 데이터에 매우 민감하다.
        1. 각 변수가 전체 훈련 데이터셋을 얼마나 잘 분류하는지 평가한다. 
        → impurity와 infromation gain을 기준으로 함.
            - impurity
                - entropy: <a href="https://www.codecogs.com/eqnedit.php?latex=H(D)&space;=&space;\Sigma&space;-P(C_j)log_2P(C_j)" target="_blank"><img src="https://latex.codecogs.com/gif.latex?H(D)&space;=&space;\Sigma&space;-P(C_j)log_2P(C_j)" title="H(D) = \Sigma -P(C_j)log_2P(C_j)" /></a>→ 클래스 하나만으로 구성된 데이터셋의 엔트로피는 0이다. 반면 모든 클래스가 균일하게 분포된 데이터셋의 엔트로피가 가장 높다.
                - gini impurity:<a href="https://www.codecogs.com/eqnedit.php?latex=Gini(D)&space;=&space;1&space;-&space;\Sigma&space;P(C_j)^2" target="_blank"><img src="https://latex.codecogs.com/gif.latex?Gini(D)&space;=&space;1&space;-&space;\Sigma&space;P(C_j)^2" title="Gini(D) = 1 - \Sigma P(C_j)^2" /></a> →  데이터 셋의 실제 레이블 분포에 따라 무작위로 레이블을 예측한다고 가정했을 때 데이터셋에서 무작위로 고른 요소의 레이블을 얼마나 자주 잘못 예측하는지 계산한 척도이다. 지니 불순도도 클래스 하나만으로 구성된 데이터셋의 엔트로피는 0이고, 반면 모든 클래스가 균일하게 분포된 데이터셋의 엔트로피가 가장 높다.
            - information gain: <a href="https://www.codecogs.com/eqnedit.php?latex=IG(D,F)&space;=&space;I(D)&space;-&space;\Sigma&space;\frac&space;{|D_S|}{|D|}I(D_S)" target="_blank"><img src="https://latex.codecogs.com/gif.latex?IG(D,F)&space;=&space;I(D)&space;-&space;\Sigma&space;\frac&space;{|D_S|}{|D|}I(D_S)" title="IG(D,F) = I(D) - \Sigma \frac {|D_S|}{|D|}I(D_S)" /></a> → 정보 이득은 특징 변수 F로 데이터셋 D를 나누었을 때 예상되는 불순도 감소량이며, 위의 공식에서 I는 불순도이다.
        2. 최적의 특징 변수, 즉 가장 정보 이득이 큰 특징 변수를 골라 트리 노드를 하나 생성하고, 특징 변수 값에 따라 이 노드에서 다시 새로운 branch를 생성한다. 
        3. 스파크는 분기가 최대 2개이다.
        4. 분기에 할당된 데이터셋이 단일 클래스로 이루어져 있거나 분기의 깊이가 일정 이상일때, 해당 분기 노드를 리프 노드(leaf node)로 만들고 이 노드에서 더 이상 새로운 분기를 만들지 않는다. 
    - 실습
        - 모델 training

            ```python
            from pyspark.ml.feature import StringIndexer
            dtsi = StringIndexer(inputCol="label", outputCol="label-ind")
            dtsm = dtsi.fit(penlpoints)
            pendtlpoints = dtsm.transform(penlpoints).drop("label").withColumnRenamed("label-ind", "label")

            #데이터셋 분할
            pendtsets = pendtlpoints.randomSplit([0.8, 0.2])
            pendttrain = pendtsets[0].cache()
            pendtvalid = pendtsets[1].cache()

            from pyspark.ml.classification import DecisionTreeClassifier
            dt = DecisionTreeClassifier(maxDepth=20)
            dtmodel = dt.fit(pendttrain)
            #maxDepth: 트리의 최대 깊이를 지정
            #maxBins: 연속적인 값을 가지는 특징 변수를 분할할 때 생성할 bin의 최대 개수를 지정한다.
            #minInstancesPerNode: 데이터셋을 분할할 때 각 분기에 반드시 할당해야 할 데이터 샘플의 최소 개수를 지정한다.
            #minInfoGain: 데이터셋을 분할할 때 고려할 정보 이득의 최저 값을 지정한다. 분기의 정보 이득이 최저 값보다 낮으면 해당 분기는 버린다. 
            ```

        - 모델 evaluation

            ```python
            dtpredicts = dtmodel.transform(pendtvalid)
            dtresrdd = dtpredicts.select("prediction", "label").rdd.map(lambda row:  (row.prediction, row.label))

            from pyspark.mllib.evaluation import MulticlassMetrics
            dtmm = MulticlassMetrics(dtresrdd)
            dtmm.precision()
            #0.951442968392121
            print(dtmm.confusionMatrix())
            ```

2. Random Forest
    - random forest: 원본 데이터셋을 무작위로 샘플링한 데이터를 사용해 의사결정 트리 여러개를 한꺼번에 학습하는 알고리즘이다.
    - 실습
        - 모델 학습

            ```python
            from pyspark.ml.classification import RandomForestClassifier
            rf = RandomForestClassifier(maxDepth=20)
            rfmodel = rf.fit(pendttrain)
            #numTrees: 학습할 트리 개수 지정
            #featureSubsetStrategy: 특징 변수 배깅 수행 방식 지정
            ```

        - 모델 평가

            ```python
            rfpredicts = rfmodel.transform(pendtvalid)
            rfresrdd = rfpredicts.select("prediction", "label").rdd.map(lambda row:  (row.prediction, row.label))
            rfmm = MulticlassMetrics(rfresrdd)
            rfmm.precision()
            #0.9894640403114979
            print(rfmm.confusionMatrix())
            ```
    </details>
    <details close>
      <summary>8.4. Using k-means clustering </summary>
      
* clustering
    - 군집화를 활용하는 예시
        - 데이터를 여러 그룹으로 분할: 예를 들어 고객 세분화(customer segmentation)나 유사한 행동을 보인 고객을 그루핑하는 작업
        - 이미지 세분화(image segmentation): 이미지 하나에서 구분 가능한 영역을 인식하고 분리
        - 이상 탐지(anomaly detection)
        - 텍스트 분류(text categorization) 또는 주제 인식(topic recognition)
        - 검색 결과 그루핑: 예를 들어 yippy 검색 엔진은 검색 결과를 범주 별로 묶어서 보여 준다.
    - 스파크에는 (1) k-means clustering, (2) 가우스 혼합 모델, (3) 거듭제곱 반복 군집화가 구현되어 있다.
        - k-means clustering: 이 방법은 군집이 구 형태가 아니거나 크기(밀도 또는 반경)가 동일하지 않을 때는 잘 작동하지 않는다. 또한 범주형 특징 변수를 다룰 수 없다.
        - 가우스 혼합 모델: 각 군집이 가우스 분포라고 가정하고, 이 분포들의 혼합으로 군집 모형을 만든다.
        - 거듭제곱 반복 군집화: spectral clustering의 일종으로 9장의 GraphX 라이브러리를 기반으로 구현되었다.
* k-means clustering
    - k-means clustering 동작 원리
        1. 데이터 포인트 k개를 무작위로 선택 → 중심점
        2. 각 중심점과 모든 포인트 간의 거리를 계산하고, 각 포인트를 가장 가까운 군집에 포함시킴.
        3. 각 군집의 평균점을 계산해 군집의 새로운 중심점으로 사용.
        4. 2,3을 반복하다가 새로 게산한 군집 중심점이 이전 중심점과 크게 다르지 않으면 반복과정을 종료함. 
    - 스파크에서 k-means clustering 사용하기
        - 먼저 데이터셋을 표준화해야한다.
        - clustering에서는 훈련 데이터셋과 검증 데이터셋으로 나눌 필요가 없다.
        - 데이터셋 훈련

            ```python
            from pyspark.mllib.linalg import DenseVector
            penflrdd = penlpoints.rdd.map(lambda row: (DenseVector(row.features.toArray()), row.label))
            penrdd = penflrdd.map(lambda x:  x[0]).cache()

            from pyspark.mllib.clustering import KMeans
            kmmodel = KMeans.train(penrdd, 10, maxIterations=5000, runs=20)
            #k: 군집 개수
            #maxIter: iteration 수(Kmeans reached the max number of iterations 메시지가 출력되면 반복횟수를 늘려야한다)
            #predictionCol: 예측 결과 칼럼 이름
            #featureCol: 특징 변수 칼럼 이름
            #tol: 수렴 허용치 -> 군집의 중심점이 움직인 거리가 이 값보다 작으면 알고리즘이 수렴했다고 판단하고 반복을 종료
            #seed
            ```

        - 모델 평가 → 군집 모델 평가는 상당히 어렵다. 정답이 없기 때문이다.
            - 군집 비용(cost value) 또는 distortion: 각 군집의 중심점과 해당 군집에 포함된 각 데이터 포인트 간 거리를 제곱해 합산한 것.

                ```python
                kmmodel.computeCost(penrdd)
                #44421031.53094221
                ```

            - 군집 중심점과 평균 거리: 군집 비용을 해당 데이터셋의 예제 개수로 나눈 값의 제곱근으로 계산한다.

                ```python
                import math
                math.sqrt(kmmodel.computeCost(penrdd)/penrdd.count())
                #66.94431052265858
                ```

            - 분할표(contingency table)

                ```python
                kmpredicts = penflrdd.map(lambda feat_lbl: (float(kmmodel.predict(feat_lbl[0])), feat_lbl[1]))

                def printContingency(rdd, labels):
                    import operator
                    numl = len(labels)
                    tablew = 6*numl + 10
                    divider = "----------"
                    for l in labels:
                        divider += "+-----"
                    summ = 0L
                    print("orig.class", end='')
                    for l in labels:
                        print("|Pred"+str(l), end='')
                    print()
                    print(divider)
                    labelMap = {}
                    for l in labels:
                        #filtering by predicted labels
                        predCounts = rdd.filter(lambda p:  p[1] == l).countByKey()
                        #get the cluster with most elements
                        topLabelCount = sorted(predCounts.items(), key=operator.itemgetter(1), reverse=True)[0]
                        #if there are two (or more) clusters for the same label
                        if(topLabelCount[0] in labelMap):
                            #and the other cluster has fewer elements, replace it
                            if(labelMap[topLabelCount[0]][1] < topLabelCount[1]):
                                summ -= labelMap[l][1]
                                labelMap.update({topLabelCount[0]: (l, topLabelCount[1])})
                                summ += topLabelCount[1]
                            #else leave the previous cluster in
                        else:
                            labelMap.update({topLabelCount[0]: (l, topLabelCount[1])})
                            summ += topLabelCount[1]
                        predictions = iter(sorted(predCounts.items(), key=operator.itemgetter(0)))
                        predcount = next(predictions)
                        print("%6d    " % (l), end='')
                        for predl in labels:
                            if(predcount[0] == predl):
                                print("|%5d" % (predcount[1]), end='')
                                try:
                                    predcount = next(predictions)
                                except:
                                    pass
                            else:
                                print("|    0", end='')
                        print()
                        print(divider)
                    print("Purity: %s" % (float(summ)/rdd.count()))
                    print("Predicted->original label map: %s" % str([str(x[0])+": "+str(x[1][0]) for x in labelMap.items()]))

                printContingency(kmpredicts, range(0, 10))
                #orig.class|Pred0|Pred1|Pred2|Pred3|Pred4|Pred5|Pred6|Pred7|Pred8|Pred9
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     0    |    1|  638|    0|    7|    0|  352|   14|    2|   23|    0
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     1    |   66|    0|    0|    1|   70|    0|    8|  573|    0|  304
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     2    |    0|    0|    0|    0|    0|    0|    0|   16|    0| 1006
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     3    |    2|    0|    0|    1|  919|    0|    0|   19|    0|    1
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     4    |   31|    0|    0|  940|    1|    0|   42|   12|    0|    1
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     5    |  175|    0|  555|    0|  211|    0|    6|    0|    3|    0
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     6    |    0|    0|    1|    3|    0|    0|  965|    0|    0|    0
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     7    |    0|    0|    4|    1|   70|    0|    1|  147|    1|  805
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     8    |    4|   15|   22|    0|   98|  385|    6|    0|  398|   31
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #     9    |  609|   10|    0|   78|  171|    0|    1|   76|    1|    9
                #----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
                #Purity: 0.666162227603
                #Predicted->original label map: ['0.0: (9, 609)', '1.0: (0, 638)', '2.0: (5, 555)', '3.0: (4, 940)', '4.0: (3, 919)', '6.0: (6, 965)', '7.0: (1, 573)', '8.0: (8, 398)', '9.0: (2, 1006)']
                ```

        - 군집개수 결정 → elbow method 사용하기
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 9</b> connecting the dots with graphX</summary>
</details>  


## 3장 spark ops

<details close>
  <summary><b>chapter 10</b> Running spark</summary><br>
    <blockquote>
    <details close>
    <summary>10.1 An overview of Spark’s runtime architecture</summary>
      
* 스파크 런타임 컴포넌트: Spark는 실행되는 동안 다음 세 종류의 process를 생성한다. 아래 세 process는 Spark runtime architecture를 구성하는 **component(컴포넌트)** 들이다.<br>
    <img width="623" alt="Screen Shot 2021-06-21 at 6 41 04 PM" src="https://user-images.githubusercontent.com/43725183/122741775-43eb9b00-d2c0-11eb-958e-60b43ef70266.png">

    Driver의 위치는 클러스터 유형과 설정에 따라 다르다. 

    1. **client-process(클라이언트)**: driver process를 생성하는 프로세스로 spark 동작을 위한 classpath, configuration option 뿐만 아니라 application 변수를 포함한다. spark-submit script, spark-shell script 또는 custom application의 형태로 제공된다.  
    2. **driver(드라이버)**: Spark application에는 반드시 한 개의 driver process가 필요하다. driver process는 SparkContext와 Scheduler를 포함하며, 전체 spark application의 execution을 총괄하는 main function과 같은 존재이다.
        - driver process와 그 하위 컴포넌트(sparkcontext, scheduler)는 
        (1) cluster manager에게 memory와 CPU resource를 요청한다.
        (2) application logic을 stage와 task 단위로 조각낸다.
        (3) 각 task를 executor들에게 나눠준다.
        (4) 각 executor의 결과를 모은다.
        - deploy mode에 따른 분류 : driver process가 client JVM에 포함되면 **client-deploy mode**라고 부르고, client JVM과 분리되어 cluster내의 하나의 JVM으로 따로 구현되면 **cluster-deploy mode**라 부른다.
        - driver process가 시작될 때 특정 application의 configuration이 반영된 `SparkContext` instance가 생성된다. 스파크 REPL 셸은 셸 자체가 드라이버 프로그램 역할을 하며, 스파크 컨텍스트를 미리 설정해 `sc`라는 변수로 제공한다. JAR파일을 제출하거나 또 다른 프로그램에서 스파크 API로 스파크 독립형 애플리케이션을 실행할 때는 애플리케이션에서 직접 스파크 컨텍스트를 생성해야 한다.  JVM당 하나의 `SparkContext`가 필요하다(여러개의 SparkContext instance를 생성할 수도 있지만, 사용하지 말자). 
        → 스파크 컨텍스트는 RDD를 생성하거나 데이터를 로드하는 등 다양한 작업을 수행하는 여러 유용한 메서드를 제공하며, 스파크 런타임 인스턴스에 접근할 수 있는 기본 인터페이스이다.
    3. **executor(실행자)**: driver로부터 받은 task를 수행하고, 그 결과를 다시 driver로 보내는 역할을 한다. executor는 여러 개의 task slot을 가지고 있는데, 이는 thread로 구현되어 병렬 작업을 가능하게 한다. 일반적으로 CPU core 수의 2,3배 값을 task slot으로 설정한다. task slot은 스레드로 구현되므로 머신의 물리적인 CPU 코어 개수와 반드시 일치할 필요는 없다. 
- 스파크 클러스터 유형
    - Spark on clusters
        - **Standalone cluster**: 스파크 자체 클러스터. 스파크 자체 클러스터는 스파크 전용 클러스터다. 이 클러스터는 오직 스파크 애플리케이션에만 적합하도록 설계되었기 때문에 kerberos 인증 프로토콜로 보호된 HDFS를 지원하지 않는다. 이러한 보안 기능이 필요할 경우에는 YARN을 사용해 스파크를 실행해야 한다. 반면 스파크 자체 클러스터는 잡 시작에 걸리는 시간이 YARN보다 더 짧다.
        - **YARN cluster**: 하둡의 리소스 매니저 및 작업 실행 시스템이다. 하둡 버전 1의 맵리듀스 엔진을 대체한 것으로 맵리듀스 버전2라고도 한다.
        - **Mesos cluster**: 메소스는 확장성과 장애 내성을 갖춘 c++기반 분산 시스템 커널이다.
    - **Spark local modes**
    : special cases of a Spark "standalone cluster" running on "**a single machine**"
        - Spark local mode
        - Spark local cluster mode
    </details>
    <details close>
    <summary>10.2. Job and resource scheduling</summary>
      
* 스파크 애플리케이션의 리소스 스케줄링은 먼저 (1) 실행자(JVM 프로세스)와 (2) CPU(태스크 슬롯) 리소스를 스케줄링한 후, 각 실행자에 (3) 메모리 리소스를 할당하는 순서로 진행한다.
- 애플리케이션의 드라이버와 실행자가 시작되면 스파크 스케줄러는 이들과 직접 통신하면서 어떤 실행자가 어떤 태스크를 수행할지 결정한다. 스파크에서는 이 과정을 **잡 스케줄링(job scheduling)** 이라고 한다.
- 스파크 애플리케이션의 리소스 스케줄링은 다음 두 가지 레벨로 이루어진다.
    1. **클러스터 리소스 스케줄링**: 단일 클러스터에서 실행하는 다수의 애플리케이션에 클러스터의 리소스를 나누어주는 작업.
    2. **스파크 잡 스케줄링**: 클러스터 매니저가 CPU와 메모리 리소스를 각 실행자에 할당하면 스파크 애플리케이션 내부에서는 잡 스케줄링이 진행된다. 잡 스케줄링은 클러스터 매니저와 관계없이 스파크 자체에서 수행하는 작업으로, 잡을 어떻게 태스크로 분할하고 어떤 실행자에게 전달할지 결정한다. **스파크는 RDD 계보를 바탕으로 잡과 스테이지, 태스크를 생성한다.** **그 다음 스파크 스케줄러는 생성한 태스크를 실행자에 분배하고 실행 경과를 모니터링한다.** 
        - CPU 리소스 분배( = task 할당)
            - (1) **선입선출(FIFO) 스케줄러** :가장 먼저 리소스를 요청한 잡이 모든 실행자의 태스크 슬롯을 필요한 만큼 전부 차지한다. FIFO는 스파크의 기본 스케줄링 모드이며, 한 번에 잡 하나만 실행하는 단일 사용자 애플리케이션에 적합하다. → 아래 그림을 보면 spark job 1만 실행중이다. <br>
                <img width="527" alt="Screen Shot 2021-06-27 at 1 35 49 PM" src="https://user-images.githubusercontent.com/43725183/123532940-97d90280-d74c-11eb-8e82-874c6c49b049.png">

                
            - (2) **공정(FAIR) 스케줄러:** 라운드 로빈 방식으로 스파크 잡들에 균등하게 리소스를 분배한다.→ 아래 그림을 보면 spark job 1, 2 모두 실행중이다. <br>
                <img width="534" alt="Screen Shot 2021-06-27 at 1 36 14 PM" src="https://user-images.githubusercontent.com/43725183/123532953-a58e8800-d74c-11eb-93dc-0a54387b92be.png">

                
            - **태스크 예비 실행(speculative execution)**: 스파크의 예비 실행은 straggler 태스크(동일 스테이지의 다른 태스크보다 더 오래 걸리는 태스크)문제를 해결할 수 있다. 예를 들어 다른 프로세스가 일부 실행자 프로세스의 CPU 리소스를 모두 점유할 때 해당 실행자는 태스크를 제 시간에 완수하지 못할 수 있다. 이때 예비 실행 기능을 사용하면 스파크는 해당 파티션 데이터를 처리하는 동일한 태스크를 다른 실행자에도 요청한다. 기존 태스크가 지연되고 예비 태스크가 완료되면 스파크는 기존 태스크의 결과 대신 예비 태스크의 결과를 사용한다.

                하지만 일부 작업에서는 예비 실행하는 사용하는 것이 적절하지 않다. 예를 들어 관계형  데이터베이스아 같은 외부 시스템에 데이터를 내보내는 작업에 예비 실행을 적용하면 두 태스크가 동일 파티션의 동일 데이터를 외부 시스템에 중복 기록하는 문제가 발생할 수 있기 때문이다. 

            - **데이터 지역성(data locality):** 스파크가 데이터와 최대한 가까운 위치에서 태스크를 실행하려고 노력하는 것을 의미한다. 스파크는 각 파티션별로 선호 위치(preferred location) 목록을 유지한다. 파티션의 선호 위치는 파티션을 저장한 호스트네임 또는 실행자 목록으로, 이 위치를 참고해 데이터와 가까운 곳에서 연산을 실행할 수 있다. 스파크는 HDFS 데이터로 생성한 RDD와 캐시된 RDD에서 사용할 때만 선호 위치 정보를 알아낼 수 있다. → 스파크가 파티션의 선호 위치 목록을 확보하면 스파크 스케줄러는 파티션 데이터를 실제로 저장한 실행자가 관련 태스크를 실행하도록 최대한 노력하고, 데이터 전송을 최소화한다. 특정 태스크의 지역성 레벨은 스파크 웹 UI Stage Details 페이지에 있는 Tasks 테이블의 Locality Level 칼럼에 표시된다.
        - 메모리 리소스 스케줄링: 스파크 실행자 JVM 프로세스 메모리는 클러스터 매니저가 할당한다(cluster-deploy mode에서는 드라이버의 메모리도 할당한다.→ local mode도!) 각 프로세스에 메모리를 할당하면 스파크는 잡과 태스크가 사용할 메모리 리소스를 스케줄링하고 관리한다.
            - 클러스터 매니저가 관리하는 메모리: 클러스터 매니저는 `spark.executor.memory` 매개변수에 지정된 메모리 용량을 실행자에 할당한다. 변수 값 뒤에 g(기가 바이트)나, m(메가 바이트) 접미사를 붙여 단위를 지정할 수 있다. 스파크는 이 메모리를 나누어 사용한다.
            - 스파크가 관리하는 메모리(버전 1.5.2. 이하): 실행자에 할당된 메모리 중 일부를 나누어서 각각 **캐시데이터**와 임시 **셔플링 데이터**를 저장하는 데 사용한다.(→ *1.6.0 버전 이후로는 실행 메모리 영역과 스토리지 영역을 통합해 관리한다.*) 캐시 스토리지 크기는 `spark.storage.memoryFraction`(default = 0.6) 로 설정하며, 임시 셔플링 공간 크기는 `spark.shuffle.memoryFraction`(default = 0.2)로 설정한다. 하지만 스파크가 메모리 사용량을 측정하고 제한하기 전에 사용량이 설정 값을 초과로 할 수 있으므로 각각 `saprk.storage.safetyFraction`(default = 0.9)과 `spark.shuffle.safetyFraction`(default = 0.8)의 값이 추가로 필요하나다. 이 변수들은 셔플링과 캐시 스토리지의 공간을 지정된 비율만큼 낮춘다.

                → 즉, 캐시 데이터 스토리지의 실제 힙 메모리 비율은 `0.6*0.9 = 54%`<br>
                → 셔플링 데이터를 임시로 저장하는 힙 메모리 비율은 `0.2*0.8 = 16%`이다.

                남은 80%는 태스크를 실행하는데 필요한 기타 자바객체와 리소스를 저장하는 데 사용한다.

            - 드라이버 메모리 설정
                - 드라이버의 메모리 리소스는 `spark.driver.memory`로 설정한다. 이 변수는 spark-shell이나 spark-submit 스크립트로 애플리케이션을 시작할 때 적용된다(클러스터 배포 모드와 클라이언트 배포 모드에 모두 적용됨)
                - 반면 다른 외부 애플리케이션의 내부 코드에서 스파크 컨택스트를 동적으로 생성할 때는 드라이버가 외부 애플리케이션의 메모리 중 일부를 사용하므로 드라이버의 메모리 공간을 늘리려면 자바의 `-Xmx` 옵션을 사용해 애플리케이션 프로세스에 할당할 자바 힙의 최대 크기를 늘려야한다.
    </details>
    <details close>
    <summary>10.3. Configuring Spark</summary>
      
스파크 환경 매개변수를 설정하는 여러가지 방법(1~4)이 있지만 어떠한 방식으로 설정하든 애플리케이션에 적용한 모든 환경 매개변수 값은 `SparkConext`의 `SparkConf` 객체에 저장된다. spark-shell은 SparkContext를 자동으로 초기화해서 sc 벼수로 제공한다. SparkConf 객체는 다음과 같이 SparkContext의 getConf 메서드로 가져올 수 있다.(`sc.getConf()`) 

1. 스파크 환경 설정 파일: 스파크 환경 매개변수의 기본 값은 `SPARK_HOME/conf/spark-defeaults.conf` 파일에 지정한다. 만약 별도의 환경 설정 파일을 사용하려면 `--properties-file` 명령줄 매개변수로 파일 경로를 변경해야 한다. 
2. 명령줄 매개변수: 스파크는 환경 설정파일로 설정한 값보다 명령줄 매개변수로 지정한 값을 우선시한다. 명령줄 매개변수와 환경 설정 파일의 매개변수는 동일 변수라도 이름이 서로 다르다. 또 전체 스파크 환경 매개변수 중 일부만 명령줄 매개변수로 지정할 수 있다. (→ 명령줄 매개변수의 전체 목록은 spark-shell또는 spark-submi의 `—-help` 옵션을 사용해 확인할 수 있다)
      
    <img width="1000" alt="Screen Shot 2021-06-27 at 1 32 57 PM" src="https://user-images.githubusercontent.com/43725183/123532894-3153e480-d74c-11eb-86e2-7f99f90a799d.png">  
    
    ```bash
    spark-shell --driver-memory 16g
    spark-shell --conf spark.driver.memory=16g
    ```

    위의 두 줄은 동일하게 드라이버의 메모리를 16g로 설정하는 명령이다. `--conf`를 사용하면 명령줄에서도 환경 설정 파일과 동일한 이름으로 매개변수를 설정할 수 있다. 이 방식으로 환경 매개변수를 여러개 지정하려면 `--conf` 또한 매개변수마다 별도로 사용해야 한다. 

3. 시스템 환경 변수: 환경 매개변수 중 일부는 `SPARK_HOME/conf/[spark-env.sh](http://spark-env.sh)` 파일로 지정할 수 있다. 이 매개변수들은 기본 값은 OS 한경 변수를 사용해 설정할 수도 있다. 시스템 환경 변수로 설정한 방식은 모든 설정 방식 중에서 가장 낮은 우선순위로 적용된다. `spark-env.sh.template`을 참고하자. 
cf. 스파크 자체 클러스터의 spark-env.sh 파일을 변경했다면 모든 실행자가 동일한 환경 설정을 참고하도록 이 파일을 모든 워커 노드에 복사해야한다. 
4. 프로그램 코드로 환경 설정: 다음과 같이 SparkConf 클래스로 프로그램 내에서 직접 스파크 환경 매개변수를 설정할 수도 있다. 그러나 런타임중에서는 변경할 수 없으므로 반드시 SparkContext 객체를 생성하기 전에 SparkConf 객체의 설정을 마쳐야 한다. 

    ```python
    from pyspark.conf import SparkConf
    from pyspark.context import SparkContext
    conf = SparkConf()
    conf.set("spark.driver.memory", "16g")
    conf.get("spark.driver.memory")  #16g
    #conf.setAppName("<new name>")  #spark app 이름 변경
    sc = SparkContext(conf)
    ```

5. master 매개변수: 애플리케이션을 실행할 스파크 클러스터의 유형을 지정한다. 

    ```bash
    spark-submit --master <master_connection_url>
    ```

    ```python
    from pyspark.conf import SparkConf
    from pyspark.context import SparkContext
    conf = SparkConf()
    conf.set("spark.master", "<master_connection_url>")
    #conf.setMaster("<master_connection_url>")  #스파크 클러스터 유형 지정
    sc = SparkContext(conf)
    ```

6. 설정된 매개변수 조회

    ```python
    sc.getConf().getAll()
    ```
    <img width="1000" alt="Screen Shot 2021-06-27 at 1 33 30 PM" src="https://user-images.githubusercontent.com/43725183/123532903-46307800-d74c-11eb-8bed-3f8d5c52c39d.png">


    </details>
    <details close>
    <summary>10.4. Spark web UI</summary><br>
      
스파크는 SparkContext를 생성하면서 동시에 스파크 웹 UI를 시작한다. 

- **Jobs** 페이지: 현재 실행중인 잡, 완료된 잡, 실패한 잡의 통계 정보를 제공한다. 각 잡의 시작 시각과 실행시간, 실행 완료된 스테이지 및 태스크 정보를 확인할 수있다.
- **Stages** 페이지: 잡의 스테이지를 요약한 정보를 제공한다. 여기서 각 스테이지의 시작 시각, 실행 시간, 실행 현황, 입출력 데이터의 크기, 셔플링 읽기, 쓰기 데이터양을 확인할 수 있다.  + 누적변수도 확인가능!

    → 각 스테이지의 Description 칼럼을 클릭하면 해당 스테이지의 세부 페이지로 이동한다. 스테이지 세부 페이지에서는 잡 상태를 디버깅하는 데 유용한 정보를 얻을 수 있다. 예를 들어 잡의 실행 시간이 예상보다 오래 걸리면 스테이지 세부 페이지를 살펴보고 지연을 유발하는 스테이지와 태스크를 찾아서 문제 범위를 좁힐 수 있다. 

    - 예1: GC Time(실행자가 자바 가비지 컬렉션을 실행하려고 태스크를 잠시 중단한 시간)이 과다하다면 실행자에 메모리 시로를 더 많이 할당하거나 RDD의 파티션 개수를 늘려야한다.(파티션을 추가하면 파티션당 요소 개수가 감소해 메모리 사용량을 줄일 수 있다)
    - 예2: 셔플링 읽기, 쓰기 처리량이 과다하다면 프로그램 로직을 변경해 불필요한 셔플링을 피해야 한다
- **Storage** 페이지: 캐시된 RDD 정보와 캐시 데이터가 점유한 메모리, 디스크 타키온 스토리지 용량을 보여준다.
- **Environment** 페이지: 스파크 환경 매개변수뿐만 아니라 자바 및 스칼라의 버전, 자바 시스템 속성, 클래스패스 정보를 확인할 수 있다.
- **Executors** 페이지: 클러스터 내 모든 실행자 목록(드라이버 포함)과 각 실행자(또는 드라이버)별 메모리 사용량을 포함한 여러 통계 정보를 제공한다. Storage Memory에 표시된 숫자는 스토리지 메모리의 용량이다.(10.2.4절의 54%)

    각 실행자의 Thread Dump 링크를 클릭하면 해당 프로세스 내 모든 스레드의 현재 스택 추적 정보를 볼 수 있다. 스택 추적 정보는 대기 및 교착 상태(deadlock) 등으로 프로그램 실행이 느려지는 현상을 디버깅하는데 유용하다.
    </details>
    <details close>
    <summary>10.5. Running Spark on the local machine</summary>
      
* **로컬 모드**: 책의 대부분의 예제를 스파크 로컬 모드로 실행했다. 스파크 로컬 모드는 아직 대용량 클러스터에 접근할 권한이 없거나 간단한 아이디어 및 프로그램을 빠르게 테스트할 때 유용하다.

    로컬 모드에서는 클라이언트 JVM 내에 드라이버와 실행자를 각각 하나씩만 생성한다. 그러나 실행자는 스래드를 여러 개 생성해 태스크를 병렬로 실행할 수 있다. <br>
    <img width="612" alt="Screen Shot 2021-06-27 at 1 30 48 PM" src="https://user-images.githubusercontent.com/43725183/123532861-e508a480-d74b-11eb-8ec7-257dd77440f1.png">

    스파크 로컬 모드는 클라리언트 프로세스를 마치 클러스터의 단일 실행자처럼 사용하며, master 매개변수에 지정된 스레드 개수는 곧 병렬 태스크 개수를 의미한다. 따라서 머신의 CPU 코어 개수보다 더 많은 스레드를 지정하면 CPU 코어를 더 효율적으로 활용할 수 있다.(→ 코어 개수의 두세 배 정도로 e.g. 쿼드 코어라면 스레드는 8~12개. 최소한 2개는 하기)

    - `local[<n>]`: 스레드 <n> 개를 사용해 단일 실행자를 실행한다.
    - `local`: 스레드 하나. `local[1]` 과 동일하다.
    - `local[*]`: 스레드 개수를 로컬 머신에서 사용가능한 CPU 코어 개수와 동일하게 설정해 단일 실행자를 실행한다. 즉, 모든 CPU 코어를 전부 사용한다. → default
    - `local[<n>, <f>]`: 스레드를 <n>개 사용해 단일 실행자를 실행하고, 태스크당 실패를 최대 <f>번까지 허용한다. 이 모드는 주로 스파크 내부 테스트에 사용한다.
- **로컬 클러스터 모드:** 주로 스파크 내부 테스트 용으로 사용. IPC가 필요한 기능을 빠르게 테스트하거나 시연할 때도 유용하다. 로컬 클러스터 모드는 Spark standalone cluster를 로컬 머신에서 실행하는 것이다. 둘의 차이는 마스터를 별도 프로세스가 아닌 클라이언트 JVM에서 실행한다는 것이다. Spark standalone cluster에서 적용할 수 있는 환경 매개변수 대부분은 로컬 클러스터 모드에도 적용된다.`local-cluster[<n>, <c>, <m>]`: 로컬 머신에서 스레드 <c>개와 <m> MB 메모리를 사용하는 실행자를 <n>개 생성해 스파크 자체 클러스터를 실행하라는 의미이다. 
→ 로컬 클러스터 모드에서는 각 실행자를 별도의 JVM에서 실행하므로 스파크 자체 클러스터와 거의 유사하다.
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 11</b> Running on a spark standalone cluster</summary>
    <blockquote>
    <details close>
    <summary>11.1. Spark standalone cluster components</summary>
      
Standalone 클러스터는 master 프로세스와 worker(또는 slave) 프로레스로 구성된다. (아래 두 프로세스는 클러스터 관련 프로세스로 스파크 런타임 컴포넌트와는 별개의 개념이다)

- **master 프로세스**: 클러스터 매니저 역할을 한다. 즉, 클라이언트가 실행에 요청한 애플리케이션을 받고, 각 애플리케이션에 워커 리소스(CPU 코어)를 스케줄링한다.
- **worker 프로세스**: 애플리케이션의 태스크를 처리할 실행자를 시작한다.(클러스터 배포 모드에서는 애플리케이션의 드라이버를 실행하는 역할도 워커가 담당한다)

→ 앞서 말했듯이 드라이버는 스파크 잡을 조정하고 모니터링하는 컴포넌트이며, 실행자는 잡의 태스크를 실행하는 컴포넌트이다. 
→ 스파크 자체 클러스터를 구성하려면 클러스터의 모든 노드가 워커의 역할을 하도록 각 노드에 스파크를 설치해야한다. 

다음의 노드 두개로 구성된 스파크 standalone 클러스터에서는 마스터 프로세스 하나와 워커 프로세스 두개를 실행하며 다음과 같은 순서로 애플리케이션을 시작한다. (아래 예시는 cluster deploy mode)<br>
<img width="556" alt="Screen Shot 2021-06-28 at 10 55 09 PM" src="https://user-images.githubusercontent.com/43725183/123648415-e5945e80-d863-11eb-8bce-da792def25bd.png">

1. 클라이언트 프로세스는 애플리케이션을 마스터에 제출한다.
2. 마스터는 1번 노드의 워커에 드라이버를 시작하라고 지시한다. 
3. 1번 노드의 워커는 드라이버 JVM을 시작한다.
4. 마스터는 두 워커에 애플리케이션의 실행자를 시작하라고 지시한다. 
5. 두 워커는 각각 실행자 JVM을 시작한다.
6. 드라이버는 실행자와 직접 통신하며 스파크 애플리케이션을 실행한다. **클러스터의 프로세스들은 더 이상 관여하지 않는다.** 

→ 각 실행자는 다수의 스레드(CPU 코어)를 할당받는다. 이는 다수의 태스크를 병렬로 실행할 수 있는 태스크 슬롯을 의미한다. 
→ 스파크 버전 1.4.0부터는 워커당 실행자를 여러개 생성할 수 있다. 그러나 JVM 힙이 너무 크고(64GB 이상) 가비지 컬렉션이 잡 성능에 영향을 미친다면 워커 프로세스의 수를 늘려야할 수도 있다.

→ 클러스터는 여러 애플리케이션을 동시에 실행할 수 있으며, 각 애플리케이션에는 드라이버와 실행자들이 독립적으로 할당된다.
    </details>
    <details close>
    <summary>11.2. Starting the standalone cluster</summary>
      
Standalone 클러스터 모드에서는 스파크 셸을 시작하기 전에 클러스터를 먼저 가동해야 한다. 클러스터가 정상 가동되면 마스터 접속 URL(`spark://master_hostname:port`)을 통해 애플리케이션을 클러스터와 연결할 수 있다. 

1. 클러스터 시작하기
    - (방법1) 셸 스크립트로 클러스터 시작: 스파크가 제공하는 시작 스크립트를 실행한다. 스크립트는 자동으로 클러스터 환경을 설정하고 스파크의 기본 환경 설정을 로드한다. 단 스크립트를 실행하려면 스파크를 클러스터 내 모든 노드의 동일한 위치에 설치해야 한다.

        스파크는 자체 클러스터의 컴포넌트를 시작하는 세 가지 시작 스크립트를 제공한다.(`SPARK_HOME/sbin`에서 찾을 수 있다.)

        - `start-master.sh`: 마스터 프로세스를 시작한다. 로그파일의 기본 경로는 `SPARK_HOME/logs/spark-{username}-org.apache.spark.deploy.master.Master-1-{hostname}.out`이다. `SPARK_HOME/conf` 폴더 아래에 있는 ``spark-env.sh`` 파일에 시스템 환경 변수를 설정할 수 있다.
        - `start-slaves.sh`: 클러스터에 등록된 워커 프로세스들을 모두 시작한다. SSH 프로토콜을 사용해 `SPARK_HOME/conf/slaves`(→ slaves(또는 workers) 파일에는 워커의 호스트네임 목록을 한 줄에 하나씩 입력한다) 파일에 나열된 모든 머신에 접속하고 워커 프로세스를 시작한다. 한 머신에서 여러 워커를 실행하려면 각 워커를 수동으로 시작하거나 `SPARK_WORKER_INSTANCES` 환경 변수를 사용해야 한다. 기본적으로 스크립트는 모든 워커 프로세스를 동시에 시작한다. 병렬 시작이 가능하려면 암호 없이 SSH에 접속 할 수 있도록 설정해야 한다.
        - `start-all.sh`: 마스터 프로세스와 워커 프로세스들을 시작한다.  → 내부적으로 `start-master.sh`를 호출한 후 `start-slaves.sh`을 호출한다.
    - (방법2) 수동으로 클러스터 시작: `SPARK_HOME/bin/spark-class` 스크립트에 스파크 마스터 클래스의 클래스 이름을 인수로 지정해서 호출해야 한다. 워커를 시작할때는 마스터 URL도 지정한다.

        ```bash
        $ spark-class org.apache.spark.deploy.master.Master
        $ spark-class org.apache.spark.deploy.worker.Worker spark://<IPADDR>:<PORT>
        ```

2. 스파크 프로세스 조회
    - 클러스터 프로세스들을 잘 시작했는지 확인하는 한 가지 방법은 JVM 프로세스 상태(JVM Process Status) 도구(`jps`)를 사용해 프로세스를 조회하는 것이다.
    - 마스터 프로세스와 워커 프로세스는 각각 `Master`와 `Worker`로 표시된다. 클러스터 내부에서 실행중인 드라이버는 `DriverWrapper`로 표시되며, `spark-submit` 명령이 시작한 드라이버는 `SparkSubmit`으로 표시된다.(spark-shell 명령도 포함) 실행자는 `CoarseGrainedExecutorBackend`로 표시된다.
3. 마스터 고가용성(master high availability) 및 복구 기능

    마스터 프로세스는 스파크 자체 클러스터에서 가장 중요한 컴포넌트이다. 마스터 프로세스를 통해 애플리케이션을 제출하고 현재 실쟁중인 애플리케이션의 상태를 조회할 수 있기 때문이다. 

    - **마스터 고가용성(master high availability)** : 마스터 프로세스가 중단되면 자동으로 재시작하는 기능이다. 스파크는 마스터 프로세스를 다시 시작할 때를 대비해 마스터가 중단되기 전까지 실행 중인 애플리케이션 및 워커 데이터를 복구할 수 있는 아래 두 가지 방법을 제공한다.
        1. 파일시스템 기반 마스터 복구: `spark.deploy.recoveryMode` 매개변수 값을 FILESYSTEM 으로 지정하면 `spark.deploy.recoveryDirectory` 매개변수에 지정된 디렉토리에 등록된 모든 워커와 실행 중인 애플리케이션 정보를 저장한다. 
        2. 주기퍼(zookeeper) 기반 마스터 복구: `spark.deploy.recoveryMode` 매개변수 값을 ZOOKEEPER 으로 지정하면 `spark.deploy.zookeeper.dir` 매개변수에 지정된 디렉토리에 등록된 모든 워커와 실행 중인 애플리케이션 정보를 저장한다. 또한 주키퍼가 설치 및 구성되어있어야 하며 `spark.deploy.zookeeper.url`  매개변수에 지정된 URL 로 주키퍼에 접속할 수 있어야 한다.
    </details>
    <details close>
    <summary>11.3. Standalone cluster web UI</summary>
      
마스터와 워커는 프로세스를 시작하면서 각 웹 UI 어플리케이션을 실행한다. 

- **마스터 웹 UI:** 클러스터 리소스(메모리 및 CPU 코어) 사용 및 잔여 현황뿐만 아니라, 워커, 애플리케이션, 드라이버 정보도 페공한다.
- **워커 웹 UI**: 마스터 웹 UI에서 각 워커 ID를 클릭하면 워커 웹 UI로 이동한다.
    </details>
    <details close>
    <summary>11.4. Running applications in a standalone cluster</summary>
      
다른 클러스터 유형과 마찬가지로 스파크 standalone 클러스터에서 스파크 클러스터를 실행하는 방법에는 (1) spark-submit 명령으로 프로그램을 제출, (2) 스파크 셸에서 프로그램을 실행하는 방법, (3) 별도의 애플리케이션에서 SparkContext 객체를 초기화 및 설정하는 방법이 있다. 어떤 방법을 사용하든 마스터 프로세스의 호스트 네임과 포트 정보로 구성된 마스터 접속 URL을 지정해야 한다. 

- **드라이버 위치**: 스파크 자체 클러스터에서는 클라이언트 배포 모드와 클러스터 배포 모드 두 가지 방법으로 스파크 애플리케이션을 실행할 수 있으며, 이에 따라 드라이버 프로세스 위치가 다르다.
    - 클라이언트 배포 모드(`—-deploy-mode client`): 드라이버를 클라이언트에서 실행. → default 값! cf. (2), (3) 방법은 클라이언트 배포 모드만 실행한다.
    - 클러스터 배포 모드(`—-deploy-mode cluster`): 드라이버 리소스를 클러스터 매니저가 관리하며, 드라이버 프로세스가 비정상으로 종료되면 애플리케이션을 다시 재시작할 수 있다. 또한 드라이버를 실행할 워커가 애플리케이션 JAR 파일에 접근할 수 있어야 하는데 하지만 어떤 워커가 드라이버를 실행할지 미리 알 수 없으므로, 클러스터 배포 모드를 사용하려면 애플리케이션 JAR 파일을 모든 워커 머신의 로컬 디스크로 복사해야 한다. 또는 애플리케이션 JAR 파일을 HDFS에 복사하고 JAR 파일 이름에 HDFS URL을 지정해야 한다. 
    → 드라이버 프로세스를 워커 프로세스 중 한 곳에 생성하며, 해당 워커의 CPU 코어를 하나 사용한다. 
    → but, 파이썬에서는 사용 불가능.
- **실행자 개수 지정**
    - SPARK_MASTER_OPTS 환경 변수에 `spark.deploy.defaultCores`, `spark.cores.max`를 이용해 실행자가 사용할 core수를 제한할 수 있다. `SPARK_WORKER_CORES` 로 애플리케이션이 할당받을 수 있는 머신당 코어 개수도 제한할 수 있다.
    - `spark.cores.max`에는 애플리케이션의 전체 코어 개수를 설정하며, `spark.executor.cores`에는 애플리케이션의 실행자당 코어 개수를 설정한다.
- **추가 classpath 항목 및 파일 지정**
    - SPARK_CLASSPATH 환경 변수를 사용하는 방법: JAR 파일이 여러개일 때 JAR 파일을 콜론(:)으로 구분해 나열한다.
    - 명령줄 옵션을 사용하는 방법: `spark.driver.extraClasspath`, `spark.executor.extraClasspath`, `spark.driver.extraLibrarypath`, `spark.executor.extraLibrarypath`
    → spark-submit의 경우는 `--driver-class-path`와 `--driver-library-path` 매개변수 사용.
    - `--jars` 매개변수를 사용하는 방법: spark-submit 스크립트의 `—-jars` 매개변수에 JAR 파일을 지정한다.(파일이 여러개일 때는 쉼표로 구분해 나열한다) 스크립트는 지정된 파일들을 워커 머신에 자동으로 복사하고, 드라이버와 실행자의 클래스패스에 추가한다.

        → 파일 위치: `file:`(지정된 파일을 각 워커에 복사한다. default), `local:`(워커 머신 내 동일한 위치의 로컬 파일을 지정한다.), `hdfs:`(HDFS 파일 경로를 지정한다. 각 워커는 HDFS에 바로 접근할 수 있다.), `http:, https:, ftp:`(파일의 URL을 지정한다)

    - 프로그램 코드로 파일을 추가하는 방법: SparkContext의 addJar 또는 addFile 메서드를 호출해 프로그램 내부에서 JAR 및 파일을 추가할 수도 있다. `--jars` 및 `--files`옵션도 내부적으로는 이 메서드를 호출하므로 앞서 설명한 대부분의 내용이 여기에도 적용된다.
    - 파이썬 파일 추가: spark-submit으로 파이썬 애플리케이션을 제출할 때는 `--py-files` 옵션을 사용해 .egg, .zip, .py를 추가할 수 있다.
    `e.g. spark-submit --master <master_url> --py-files file1.py,file2.py,main.py`
- **애플리케이션 강제 종료**
    - 클라이언트 배포 모드: 클라이언트 프로세스를 강제 종료하여 애플리케이션을 강제 종료한다.
    - 클러스터 배포 모드: `spark-class org.apache.spark.deploy.Client kill <master URL> <driver_ID>`
- **애플리케이션 자동 재시작**: 클러스터 배포 모드로 애플리케이션을 제출할 때 명령줄에 `--supervise`옵션을 지정하면, 스파크는 드라이버 프로세스에 장애가 발생하거나 비정상 종료될 때 드라이버를 재시작한다(→ 처음부터 다시 시작).
    </details>
    <details close>
    <summary>11.5. Spark History Server and event logging</summary>
      
- **이벤트 로깅 기능**: 스파크는 웹 UI를 표시하는데 필요한 이벤트 정보를 spark.eventLog.dir(default: /tmp/spark-events) 매개변수에 지정된 폴더에 기록한다. 스파크 마스터 웹 UI는 이 폴더 정보를 스파크 웹 UI와 동일한 방식으로 표시할 수 있으며, 해당 애플리케이션이 종료된 후에도 잡, 스테이지, 태스크의 데이터르르 마스터 웹 UI에서 확인할 수 있다. 이벤트 로깅 기능은 spark.eventLog.enabled를 true로 설정해 활성화한다.
- **스파크 히스토리 서버**: sbin아래 `start-history-server.sh` 스크립트로 시작하며 `stop-history-server.sh`로 중지할 수 있다. 기본 포트는 18080번이다.
    - `SPARK_DEAMON_MEMORY`: 히스토리 서버에 할당할 메모리 리소스를 지정한다.
    - `SPARK_PUBLIC DNS`: 서버의 공개주소를 설정한다.
    - `SPARK_DEAMON_JAVA_OPTS`: 서버에 JVM에 매개변수를 추가한다.
    - `SPARK_HISTORY_OPTS`: spark.history.로 시작하는 다양한 매개변수를 지정해 서버에 전달할 수 있다.( → spark-defaults.conf 파일에도 설정할 수 있다)
    </details>
    <details close>
    <summary>11.6. Running on Amazon EC2</summary>
      
1. 사전 준비
    - **AWS 보안 액세스 키 발급받기**: 물론 root user 계정의 키를 AWS API에도 사용할 수는 있지만 보안상 권장하지 않는다. 더 권한이 낮은 사용자를 생성하고 이 사용자의 키를 따로 생성해 사용하는 편이 좋다.
        1. IAM 페이지에서 **사용자** 페이지로 이동. **사용자 추가** 버튼 누르기 → 사용자 이름에 **sparkuser**를 입력하고, 액세스 유형에서 **프로그래밍 방식 액세스**에 체크한 **다음:권한** 버튼을 누르자.
        2. **기존 정책 직접 연결** 아이콘을 클릭한 후 정책 목록 중에서 AmazonEC2FullAccess만 선택하면 된다. 그리고 **다음:검토**를 누르자.
        3. 검토 단계에서 사용자 이름과 권한을 확인하고 사용자 만들기 버튼을 누르면 마지막 완료 창으로 이동한다.
        4. 발급된 sparkuser 사용자의 액세스 키ID와 비밀 액세스 키가 발급된 것을 확인할 수 있다. → **.csv 다운로드** 버튼을 누르면 키 정보를 내려받을 수 있다. .csv 파일을 안전한 장소에 보관하자.
    - **키 페어(key pair)** 생성: 키 페어는 클라이언트와 AWS 서비스간 통신을 보호하는 데 필요하다.
        1. EC2 페이지에서 왼쪽 메뉴 중 **네트워크 및 보안>키 페어**를 선택한 후 화면 오른쪽 맨 위에서 region을 선택하자. 한 region에서 생성한 키는 다른 region에서 동작하지 않기 때문에 올바른 region을 선택하는 것이 중요하다. 
        2. **키 페어 생성** 버튼을 누르고 키 페어 이름을 저장하자.(e.g. SparkKey) 키 페어를 생성하면 브라우저는 프라이빗 키(private key)가 담긴 `<key_pair_name>.pem`파일을 자동으로 내려받는다. 
        3. 접근 권한 변경: `chmod 400 SparkKey.pem`
        4. 키가 유효한지 확인: `openssl rsa -in SparkKey.pem -check`
2. EC2 기반 스파크 standalone 클러스터 생성
    - spark-ec2 스크립트: EC2 클러스터를 관리하는 메인 스크립트로 [https://goo.gl/m5A4oi](https://goo.gl/m5A4oi) 에서 다운로드 받을 수 있다. SPARK_HOME 폴더에 ec2 이름으로 디렉터리를 하나 생성하고, AMPLab의 spark-ec2 깃허브 저장소를 이 디렉토리로 복사하자. spark-ec2 스크립트는 다음과 같이 호출한다.→ `spark-ec2 options action cluster_name`
        <img width="596" alt="Screen Shot 2021-07-04 at 3 42 33 PM" src="https://user-images.githubusercontent.com/43725183/124375689-75ac2b00-dcde-11eb-88e2-005af70a6f59.png">

        위의 각 액션을 실행하려면 생성할(또는 생성할) 클러스터를 지칭하는 cluster_name과 security credentials를 인수로 전달해야 한다. 

    - 보안 자격 증명(security credentials) 지정

        ```bash
        export AWS_SECRET_ACCESS_KEY=<your_AWS_access_key>
        export AWS_SECRET_KEY_ID=<your_AWS_key_id>
        ```

        위와 같이 환경변수를 설정하고, spark-ec2 스크립트의 `—-key-pair` 옵션(줄여서 -k)에 **키 페어 이름**을 지정하고, `--identity-file`옵션(줄여서 -i)에 프라이빗 키가 담긴 **pem 파일**을 전달한다. 정리하면 다음과 같다.

        ```bash
        spark-ec2 -k SparkKey \
         -i SparkKey.pem \
         -r eu-west-1 \
         launch spark-in-action
        ```

    - 인스턴스 유형 변경: spark-ec2 스크립트가 생성하는 EC2 인스턴스 기본 유형은 m3.large로 코어 두 개와 RAM 7.5GB로 구성된다. 기본설정대로하면 마스터와 슬레이브 머신에 동일하게 m3.large가 사용된다. 보통은 마스터가 리소스를 더 적게 쓰기 때문에 비용 낭비를 가져올 수 있다.
        - 슬레이브 유형: `--instance-type`(줄여서 -t)로 t2.small로 낮추자.
        - 마스터 유형: `--master-instance-type`(줄여서 -m)로 t2.micro로 낮추자.
    - 하둡 버전 변경: spark-ec2는 기본으로 하둡 1.0.4를 설치하는데 이를 변경하자. `--hadoop-major-version`의 매개변수를 2로 바꾸면 스크립트는 하둡 2.0.0 MR1이 포함된 클라우데라 CDH 4.2.0용 스파크를 설치한다.
    - 보안 그룹 설정: 기본적으로 EC2 인스턴스는 인터넷 포트를 통해 접근할 수 없다. 따라서 기본 설정을 바꾸지 않으면 스**파크 EC2 클러스터 외부에 위치한 클라이언트 머신에서 애플리케이션을 제출할 수 없다.** 따라서 EC2 보안 그룹(security group)을 추갈 설정해 EC2 머신이 외부 인터넷과 통신할 수 있도록 인바운드 및 아웃바운드 규칙을 변경해야 한다. 
    → EC2 페이지에서 왼쪽 메뉴의 network&security 아래에 있는 **보안 그룹** 메뉴를 선택하자. **보안 그룹 생성** 버튼을 누르면 보안 그룹을 생성할 수 있는 대화상자를 표시한다. 보안 그룹 이름을 설정하자(e.g. Allow7077) 
    → 그러나 이처럼 인터넷 전체를 대상으로 포트를 개방하는 것은 권장하지 않으며, 오직 테스트 환경에서만 한시적으로 사용해야 한다. 운영 환경에서는 특정 주소만 접근할 수 있도록 제한하는 것이 좋다.
    - 클러스터 시작: 스크립트는 보안 그룹을 생성하고 인스턴스를 생성한 후 스칼라, 스파크, 하둡, 타키온 패키지를 설치한다. 스크립트는 이 패키지들을 깃허브 저장소에서 내려받고 rsync프로그램을 사용해 각 워커에 분산 배포한다.

        ```bash
        ./spark-ec2 \
        --key-pair=SparkKey \
        --identity-file=SparkKey.pem \ 
        --slaves=3 \                #slave 수 3개로 지정
        --region=eu-west-1 \
        --instance-type=m1.medium \ 
        --master-instance-type=m1.small \
        --hadoop-major-version=2 \ 
        launch spark-in-action
        ```
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 12</b> Running on YARN and Mesos</summary><br>
</details>  

## 4장 Bringing it together
<details close>
<summary><b>chapter 13</b> case study</summary><br>
<blockquote>
  <details close>
    <summary>13.1. Understanding the use case</summary>
    
1. 예제 시나리오
    - 실시간 대시보드: 센서 데이터, 리소스 사용 현황 데이터, 클릭스트림 데이터(사용자가 웹 사이트에 탐색한 흔적을 기록한 로그) 등을 시각화할 수 있다.
    - 이 장에서는 접속 로그 파일의 형태로 유입되는 클릭스티림 데이터를 분석하고, 그 결과를 웹 페이지에 표시하는 **웹 통계 대시보드**를 구현한다. 애플리케이션은 접속 로그 파일을 수신하고, 초당 활성 사용자(active user) 세션 개수와 초당 요청(request) 횟수, 오류 발생 횟수, 광고 클릭 횟수를 계산한 후 이 모든 정보를 실시간 그래프로 그린다.

        <img width="478" alt="Screen Shot 2021-07-29 at 6 00 52 PM" src="https://user-images.githubusercontent.com/43725183/127463721-27061e6a-e642-4c6e-9173-032aac71a8fe.png">

        위쪽 그래프는 활성 사용자의 세션 개수를 보여주며, 아래 쪽 그래프는 초당 요청 횟수, 오류 발생 회수, 광고 클릭 횟수를 보여준다. 오른쪽 텍스트 영역에는 가장 최근에 입수한 메시지 100개가 출력되며, 이를 디버깅에 활용할 수 있다. 웹 페이지의 왼쪽 위에 위치한 stop 버튼은 메시지 입수를 시작하거나 중지하는 데 사용한다. 또 위쪽에 있는 버튼들을 눌러 그래프 시간 범위를 변경할 수도 있다. 

    - 예제 데이터: date and time, IPaddress, sessionId, URL, method, respCode, duration

        <img width="479" alt="Screen Shot 2021-07-29 at 6 01 08 PM" src="https://user-images.githubusercontent.com/43725183/127463753-4ff19ae2-84c5-4db8-8f40-486f74be1fc1.png">

2. 예제 애플리케이션의 컴포넌트

    <img width="511" alt="Screen Shot 2021-07-29 at 6 01 29 PM" src="https://user-images.githubusercontent.com/43725183/127463811-7ae2d95a-4dcd-4a29-906c-eedd11f68a26.png">

    - **로그 시뮬레이터(Log simulator)**: 실습 환경에서는 실제 운영 중인 웹 사이트의 접속 로그를 확보할 수 없으므로 로그 시뮬레이터를 사용한다. 로그 시뮬레이터는 가상의 웹 사이트 URL을 방문한 사용자들의 접속 로그를 앞서 정의한 형식으로 생성하고, 카프카 토픽에 바로 전송한다. 
    cf. 실제 웹 사이트의 클릭스트림 데이터를 다루려면 로그 파일을 수집하고 카프카로 전송할 방법이 필요한데, 아파치 플럼이 좋은 대안이 될 수 있다. 플럼은 tail -F 명령을 실행하고 그 결과를 카프카로 전송할 수 있다.
    - **로그 분석 모듈(Log Analyzer)**: 로그 분석 모듈은 카프카에서 로그 데이터를 읽어 들인 후 예제 애플리케이션의 각종 통계를 계산하고, 계산 결과를 다시 카프카의 다른 토픽에 기록한다. 로그 분석 모듈이 계산하는 통계 결과는 두 번째 카프카 토픽에 전송되며, 메시지 형식은 `<timestamp>:(key->value,...)` 이다. 
    cf. timestamp는 long타입의 숫자로, 1970년 1월 1일 0시 0분 0초부터 해당 통계가 기록된 시각 사이의 밀리초(millisecond) 수를 계산한 것이다.

        <img width="474" alt="Screen Shot 2021-07-29 at 6 01 42 PM" src="https://user-images.githubusercontent.com/43725183/127463843-796069a8-93dd-4556-8f72-197a7f405b2a.png">

        광고 통계를 계산할 때는 모든 광고 배너가 /ads/<ad_category>/<ad_id>/clickfw 형식의 URL과 연결되었다고 가정한다. 사용자가 광고를 클릭하면 브라우저는 이 URL로 먼저 이동해서 해당 광고의 클릭 이벤트를 기록한 후 광고 제휴 사이트로 다시 이동(redirect)한다. 

    - **웹 통계 대시보드(Web stats dashboard)**: 카프카에서 통계 데이터를 가져온 후 웹 소켓으로 클라이언트 웹 브라우저에 전송한다. 그리고 D3.js 자바스크립트 라이브러리를 사용해 통계 데이터를 실시간 그래프에 표시한다. 웹 브라우저에서 실행하는 자바스크립트 코드는 실시간 메시지 순서가 뒤섞이지 않도록 타임 스탬프별로 통계 데이터를 집계해 정렬한다.
  </details>
  <details close>
    <summary>13.2. Running the application</summary>
    
1. 가상 머신에서 애플리케이션 시작(생략)
2. 애플리케이션을 수동으로 시작
    1. 아카이브 내려받기: 각 컴포넌트의 프로젝트와 소스파일은 책의 깃허브 저장소에 있는 ch13 폴더에서 가져올 수 있다. → 로그 시뮬레이터, 로그 분석 모듈, 웹 통계 대시보드
    2. 카프카 토픽 생성
        - (1) 로그 이벤트를 저장할 카프카 토픽(weblogs)과 (2) 통계 데이터(stats)를 저장할 카프카 토픽을 생성해야 한다.
        - start-kafka.sh 스크립트는 복제 계수를 1로 설정하고 각 토픽별로 파티션을 한 개만 사용했다. 다른 값을 사용하려면 다음과 같이 입력한다.(명령을 실행학 전에 먼저 주키퍼와 카프카를 시작해야 한다)

            ```bash
            $ kafka-topics.sh --create \
            --topic <topic_name>
            --replication-factor <repl_factor>
            --partitions <num_partitions>
            --zookeeper <zk_ip>:2181
            ```

    3. 로그 분석 모듈 시작
        - 로그 분석 모듈의 잡은 일반 스파크 잡과 마찬가지로 spark-submit 명령에 StreamingLogAnalyzer 클래스를 지정해 시작한다. 애플리케이션 JAR 파일 뒤에 brokerList와 checkpointDir을 필수 인수로 전달해야 한다.
            - brokerList: 카프카 설치 위치를 지정한다. 카프카 브로커의 IP 주소와 포트 번호를 입력하며, 브로커를 여러개 지정할 때는 각 브로커 주소를 쉼표로 구성한다.
            - checkpointDir: 스파크의 체크포인트 데이터를 저장한 디렉터리 URL을 지정한다.
        - 로그 분석 모듈을 제출하는 명령은 다음과 같다

            ```bash
            $ spark-submit --master <your_master_url> \
            --class org.sia.loganalyzer.StreamingLogAnalyzer \
            streaming-log-analyzer.jar -brokerList=<kafka_ip>:<kafka_port>
            -checkpointDir=hdfs://<hdfs_host>:<hdfs_port>/<checkpoint_dir>
            ```

            추가로 다음 선택 매개변수를 지정할 수 있다.

            - inputTopic: 로그 이벤트가 유입될 입력 토픽 이름
            - outputTopic: 통계 데이터를 내보낼 출력 토픽 이름
            - sessionTimeout: 세션 시간 초과를 판단하는 기준(단위: 초). 각 세션의 마지막 요청 시점부터 경과한 시간이 이 변수 값보다 크면 해당 세션이 종료되었다고 판단한다.
            - numberPartitions: 통계 연산에 사용할 RDD 파티션 개수
    4. 웹 통계 대시보드 시작
        - 웹 통계 대시보드는 자바 웹 애플리케이션이므로 웹 소켓을 지원하는 모든 종류의 자바 애플리케이션 서버에서 실행할 수 있다. 어떤 종류의 서버를 사용하든 다음 두 가지 자바 시스템 변수를 반드시 지정해야 한다. (실습에서는 IBM 웹스피어 리버티 프로파일 서버 사용)
            - zookeeper.address: 카프카에 접속하는 데 사용할 주키퍼의 호스트 네임과 포트 번호
            - kafka.topic: 통계 메시지를 읽어들일 토픽 이름
        - 웹 애플리케이션 접속 URL: 애플리케이션 서버 및 설치 방법에 따라 다르다. 따라서 URL이 정해지면 webstats.js 파일에 잇는 URL 또한 이에 맞추어서 바꾸어야 한다.
        - 웹 통계 대시보드는 카프카에서 메시지를 가져와 그래프에 표시한다. 또 애플리케이션 서버의 시스템 출력 로그 파일에 메시지를 출력한다.
    5. 로그 시뮬레이터 시작: 컴포넌트를 모두 실행했다면 이제 로그 시뮬레이터를 시작할 수 있다. 

        ```bash
        $ ./start-simulator.sh --brokerList=<kafka_host>:<kafka_port>
        #별도의 카프카를 사용한 경우 위와 같이 카프카 주소를 전달해야 한다. 
        ```
  </details>
    <details close>
    <summary>13.3. Understanding the source code</summary>
  </details>
    </blockquote>
      </details>    
 <details close>
  <summary><b>chapter 14</b> DL on spark with H2O</summary><br>
  </details>    

</details>
      
### Appendix
<details close>
<summary><b>A. Installing Apache Spark</b></summary>
  
1. 자바(JDK) 설치
    - 자바가 설치되어 있는지 확인하기: `which javac`
    (이 명령은 javac 명령을 실행했을 때 실제로 호출될 실행 파일 위치를 반환한다.)
    - (만약 자바가 설치되어 있다면)JAVA_HOME 환경 변수가 올바르게 설정되어 있는지 확인해본다.

        ```bash
        #환경 변수 설정되어 있는지 확인
        $echo $JAVA_HOME #open-jdk 설치 폴더

        #환경 변수 설정
        $echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" | sudo tee /etc/profile.d/sia.sh
        $source /etc/profile.d/sia.sh 
        ```

    - (만약 자바가 설치되어 있지 않다면)JDK 설치하기:  `sudo apt-get -y install openjdk-8-jdk`
2. 아파치 스파크 내려받고 설치 및 설정
    - 스파크 내려받기 페이지([http://spark.apache.org/downloads.html](http://spark.apache.org/downloads.html))에서 <br>
    (1) 최신 스파크 릴리스 선택(Choose a Spark release),<br>
    (2) 최신 하둡 버전으로 빌드된 패키지 유형 선택(Choose a package type),<br>
    (3) 스파크 다운로드(Download Spark)<br>
    아파치 미러의 목록이 나오면 가장 상위에 추천된 미러 사이트를 클릭한다.
    - 압축 풀고 설치: 리눅스의 표준 규칙에 따라 사용자 홈 디렉토리 아래의 bin 디렉토리를 스파크 바이너리 위치로 사용한다. 홈 디렉토리에는 전체 권한(읽기, 쓰기, 실행)이 부여되므로 매번 sudo를 입력할 필요가 없다.

        ```bash
        $cd $HOME/Downloads
        $tar -xvf spark*
        $rm spark*tgz

        ##홈디렉토리 아래의 bin 디렉토리로 스파크 바이너리 옮기기
        $cd $HOME
        $mkdir -p ~/bin/sparks
        $mv Downloads/spark-* bin/sparks
        ```

    - 여러 버전의 스파크를 사용할 경우 bin 디렉토리 아래에 sparks 디렉토리를 생성하고 이 안에 스파크의 현재 및 미래 버전을 모두 저장하자. 그리고 현재 사용할 버전에 심볼릭 링크를 만들자.

        ```bash
        $cd $HOME/bin
        $ln -s sparks/spark-2.0.0-bin-hadoop2.7 spark
        $tree -L 2 #심볼릭 링크 확인. 여기서 spark 폴더는 sparks 폴더 내의 다른 폴더를 가리키는 심볼릭 링크를 가리킨다. 

        #심볼릭 링크 바꾸기(기존의 심볼릭 링크 지우고, 새로 심볼릭 링크 만들기)
        $rm spark
        $ln -s spark-1.6.1-bin-hadoop2.6 spark
        ```

3. 스파크 셸
    - 스파크 셸 시작하기

        ```bash
        $cd $HOME/bin/spark
        $./bin/spark-shell 
        ```

    - 로깅레벨 변경하기: 스파크의 LOG4J 설정을 변경해 스파크 셸에는 오류 로그만 출력하고, 나머지 로그는 추후 문제 진단에 사용할 수 있도록 스파크 루트 폴더 아래의 logs/info.log 파일에 저장해보자.

        ```bash
        $gedit conf/log4j.properties  #스파크의 conf 디렉터리 아래에 log4j.properites 파일을 생성. log4j.properties 파일은 ch2 참조
        ```
    - 스파크 REPL(=스파크 셸)은 스파크 컨텍스트와 SparkSession 객체를 각각 sc와 spark라는 변수로 제공한다. 스파크 애플리케이션은 SparkSession을 통해 스파크에 접속하고 세션 설정, 작업 실행 관리, 파일 읽기 및 쓰기 작업 등을 수행할 수 있다. 스파크 REPL에 spark.까지만 입력하고 tab을 누르면 SparkSession이 제공하는 모든 함수를 볼 수 있다.
    - 파이썬 셸로 spark를 시작하는 경우: `$pyspark`
</details>
<details close>
  <summary><b>B. Understanding MapReduce</b></summary>
(MapReduce: Simplified Data Processing on Large Clusters)

- Job을 잘게 분할하고 클러스터의 모든 노드로 **map**ping, 즉 파견해 분산 처리를 수행하는 새로운 패러다임. 각 노드는 분할된 job을 처리한 중간 결과를 생성한다. 그 다음 분할된 중간 결과를 **reduce**, 즉 집계해 최종 결과를 낸다.
    - 맵리듀스는 최소한의 API만 노출해 대규모 분산 시스템을 다루는 복잡한 작업을 효과적으로 감추었다. map과 reduce만 노출함.
    - 맵리듀스는 데이터를 굳이 옮겨서 처리할 필요가 없다. 대신 데이터가 저장된 곳으로 "프로그램"을 전송한다.
- **Word count 문제**(빅데이터 분야의 hello world같은 존재)
한 웹 사이트에서 흔히 사용하는 단어를 분석해달라.

    ```
    Is it easy to program with MapReduce?
    Is it as easy as Map and Reduce are.
    ```

    1. `map`: 각 문장을 읽고 단어로 분할한다. 그다음 대문자를 소문자로 변환하고, 구두점을 제거한 후 각 단어마다 1이라는 값을 생성한다. 즉, key가 단어이고 value가 count(=1)이다. 
    → map은 입력된 각 문장별로 키와 값을 받아 키-값 쌍의 목록을 반환한다. 
    2. **shuffle phase:** 동일한 단어를 동일한 리듀서에 전달한다. 거의 모든 애플리케이션에서 가장 좁은 병목으로 작용한다. 
    3. `reduce`: map 함수의 return값을 받고, 단어별 출현 횟수를 합산한 후 최종 결과를 생성한다. 
- (정리) map 함수는 키-값 쌍을 가져와 임의의 변환 연산을 적용하고 중간 결과물로 키-값 쌍의 목록을 반환한다. 그런 다음 맵리듀스 API는 map 함수가 반환한 키-값 쌍을 키별로 그루핑해 reduce 함수의 입력으로 전달한다. reduce 함수는 입력된 값들을 임의의 방법으로 병합하고 그 결과를 최종 출력파일에 쓴다. 모든 리듀서가 작업을 완료하면 사용자 프로그램으로 제어를 반환한다.
</details>
<details close>
  <summary><b>C. A primer on linear algebra</b></summary><br>
  
- 행렬과 벡터
- 행렬 덧셈
- 스칼라배
- 행렬곱셈
- 단위행렬
- 역행렬: 역행렬을 갖는 정사각 행렬을 non-singular matrix라고 한다.
</details>

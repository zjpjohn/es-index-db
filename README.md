# db-river-es
<h1>背景</h1>
<p style="font-size: 14px;">  <a href="https://github.com/elastic/elasticsearch">ElasticSearch</a>, 深受欢迎的开源分布式搜索引擎，很多场景下，我们需要将数据库的数据导入到ES，提供快速稳定的搜索服务。然而，从绑定了很多业务逻辑的关系型数据库中创建规范的，能够搜索的索引并不那么容易，我们必须根据业务，编写较多代码，关联多个表，才能很好的创建索引，而且很多时候这些代码是重复的。
<p style="font-size: 14px;">  另外索引创建之后，数据库数据如有改动，索引无法联动修改，ElasticSearch没有相关功能，我们只能根据修改频率重建索引，基本上没有实时性可言。</p>
<p style="font-size: 14px;">  db-river-es正是为了解决该问题而生。</p>
<h1>项目介绍</h1>
<p style="font-size: 14px;">   名称：db-river-es</p>
<p style="font-size: 14px;">   语言：纯java开发</p>
<p style="font-size: 14px;">   定位：从数据库创建ElasticSearch全量索引，索引与数据库数据联动，实时更新</p>
<p style="font-size: 14px;">   关键词：ElasticSearch index / real time index </p>
<h1>文档</h1>
<p style="font-size: 14px;"> <a href="https://github.com/wxingyl/db-river-elasticsearch/wiki">here</a></p>

# Topnew CMS

Topnew CMS is a tiny MVC frame work which contains only one file: **vendor/topnew/src/Cms.php**, it is only 5kb in size.

Topnew\CMS is actually a router + controller, without Modal or View. It uses Topnew\Db to do all database tasks for the modal, and it uses Blade or Twig for view.

There are several optional supporting classes to make this tiny MVC working smoothly:

* Topnew\Ajax -- api ajax calls
* Topnew\Auth -- login authentication
* Topnew\Chart-- produce nice charts
* Topnew\Data -- handle all POST / GET data veridation and sanitization
* Topnew\Db   -- handle all database tasks

For wiki docs as how to use above classes, please visit: http://topnew.net

Here is a quick example of Topnew\Db:

$config = ['host' => 'localhost', 'user' => 'root', 'pass' => 'secret'];

$db = new Topnew\Db($config);

$data = $db->select('id, first_name, last_name, gender')

->from('users AS u')

->join(['c' => 'companies'], 'c.id = u.company_id')

->where('c.company_name', 'like', 'google')

->where('u.name', '!=', ['Ben', 'John'])

->order('u.last_name')

->limit(200)

->all();

$id = $db->insert('users', ['first_name' => 'Topnew', 'last_name' => 'Geo', ]);

>>  $db->where('id', '>', 5000)->where('gender', 'Male')->delete('users');

>>  $db->where('id', 1)->update('users', ['first_name' => 'Ben',]);

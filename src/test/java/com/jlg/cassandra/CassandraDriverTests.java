package com.jlg.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = CassandraApplication.class)
public class CassandraDriverTests {

  @Autowired
  Session session;

  @Before
  public void before() {
    removeMyStatusKeySpace();
    createMyStatusKeySpace();
  }

  @Test
  public void should_create_key_space() {
    createMyStatusKeySpace();
  }

  @Test
  public void should_create_users_table() {
    createUsersTable();
  }

  @Test
  public void should_remove_key_space() {
    removeMyStatusKeySpace();
  }

  @Test
  public void should_insert_user_record() {
    createUsersTable();
    insertUserRecord();
  }

  @Test
  public void should_remove_user_record() {
    createUsersTable();
    insertUserRecord();
    deleteUserRecord();
  }

  private void createMyStatusKeySpace() {
    String cql = "CREATE KEYSPACE my_status WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}";
    session.execute(cql);
  }

  private void removeMyStatusKeySpace() {
    String cql = "DROP KEYSPACE my_status";
    try {
      session.execute(cql);
    } catch (InvalidConfigurationInQueryException e) {
      System.out.println(e);
    }
  }

  private void createUsersTable() {
    String cql = "CREATE TABLE my_status.users ( \"username\" text PRIMARY KEY, \"email\" text)";
    session.execute(cql);
  }

  private void insertUserRecord() {
    String cql = "INSERT INTO my_status.users (username, email) VALUES ('jgaerke', 'jgaerke@gmail.com')";
    session.execute(cql);
  }

  private void deleteUserRecord() {
    String cql = "DELETE FROM my_status.users WHERE username = 'jgaerke'";
    session.execute(cql);
  }
}

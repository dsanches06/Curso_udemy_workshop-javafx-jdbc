package db;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DB {

	private static Connection conn = null;

	//criar e obter conexão a base de dados
	public static Connection getConnection() {
		if (conn == null) {
			try {
				// obter as propriedades
				Properties props = loadProperties();
				// obter a url da base de dados
				String url = props.getProperty("dburl");
				// conectar com a base de dados
				conn = DriverManager.getConnection(url, props);
			} // se der errado
			catch (SQLException e) {
				// cria uma nova excepção
				throw new DbException(e.getMessage());
			}
		}
		return conn;
	}

	//fechar a conexao ao banco de dados
	public static void closeConnection() {
		if (conn != null) {
			try {
				// fechar uma conexão
				conn.close();
			} // se der errado
			catch (SQLException e) {
				// cria uma nova excepção
				throw new DbException(e.getMessage());
			}
		}
	}

	// carregar um metodo para carregar o db.properties
	private static Properties loadProperties() {
		// usar o file input stream para carregar o ficheiro
		try (FileInputStream fs = new FileInputStream("db.properties")) {
			// criar uno novo obecto do tipo Properties
			Properties props = new Properties();
			// vai carregar os dados do ficheiro para o objecto props
			props.load(fs);
			// retorna o objecto
			return props;
		} // se der errado
		catch (IOException e) {
			// cria uma nova excepção
			throw new DbException(e.getMessage());
		}
	}

	public static void closeStatement(Statement stat) {
		if (stat !=null) {
			try {
				stat.close();
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
	}
	
	public static void closeResultSet(ResultSet rs) {
		if (rs !=null) {
			try {
				rs.close();
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
	}
}

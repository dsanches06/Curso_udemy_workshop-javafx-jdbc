package model.dao.Impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {

	private Connection conn;

	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {
		// criar um prepared statement
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("INSERT INTO department (Name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
			// setar valor de ?
			st.setString(1, obj.getName());
			// execute update to return rows affect
			int rowsAffected = st.executeUpdate();
			// if rowAffected > 0
			if (rowsAffected > 0) {
				// cria um result set para obter o id gerado
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					// obter id gerada
					int id = rs.getInt(1);
					// atribuir id para o department
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			} else {
				throw new DbException("Unexpected error! No rows affected!");
			}

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Department obj) {
		// criar um prepared statement
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("UPDATE department SET Name = ? WHERE Id = ?");

			// setar valor de ?
			st.setString(1, obj.getName());
			st.setInt(2, obj.getId());

			// execute update to return rows affect
			st.executeUpdate();

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteById(Integer id) {
		// criar um prepared statement
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("DELETE FROM department WHERE Id = ?");

			// setar valor de ?
			st.setInt(1, id);

			// execute update to return rows affect
			st.executeUpdate();

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public Department findById(Integer id) {
		// criar um prepared statement
		PreparedStatement st = null;
		// criar um result set
		ResultSet rs = null;
		try {
			st = conn.prepareStatement("SELECT * FROM department WHERE Id = ?");

			// setar valor de ?
			st.setInt(1, id);

			// execute query to return rows affect
			rs = st.executeQuery();
			// testar se existir pelo menos 1 dados
			if (rs.next()) {
				// criar um departamento
				Department obj = instantiateDepartment(rs);
				// retorna departmento
				return obj;
			}
			return null;

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public List<Department> findAll() {
		// criar um prepared statement
		PreparedStatement st = null;
		// criar um result set
		ResultSet rs = null;
		try {
			st = conn.prepareStatement("SELECT * FROM department");

			// execute query to return rows affect
			rs = st.executeQuery();

			// criar uma lista
			List<Department> list = new ArrayList<>();
			// testar se existir pelo menos 1 dados
			while (rs.next()) {
				// criar um departamento
				Department obj = instantiateDepartment(rs);
				// adicionar na lista
				list.add(obj);
			}
			return list;

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("Id"));
		dep.setName(rs.getString("Name"));
		return dep;
	}

}

package model.dao.Impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {

	private Connection conn;

	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller obj) {
		// prepared statement
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(
					"INSERT INTO seller(Name, Email, BirthDate, BaseSalary, DepartmentId) VALUES (?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);

			// setar valores
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());

			// executa as query update
			int rowsAffect = st.executeUpdate();

			if (rowsAffect > 0) {
				// receber o id gerado na inserção
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					// obter o id da tabela
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			} else {
				throw new DbException("Unexpected error: No rows affected!");
			}

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Seller obj) {
		// prepared statement
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(
					"UPDATE seller SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? "
							+ "WHERE Id = ?");
			// setar valores
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			st.setInt(6, obj.getId());

			// executa as query update
			st.executeUpdate();

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteById(Integer id) {
		// prepared statement
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("DELETE FROM seller WHERE Id = ?");
			// atribuir o valor de ?
			st.setInt(1, id);
			// executar update query
			st.executeUpdate();
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public Seller findById(Integer id) {
		// prepared statement
		PreparedStatement st = null;
		// result set
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT seller.*, department.Name as DepName " + "FROM seller INNER JOIN department "
							+ "ON seller.DepartmentId = department.Id " + "WHERE seller.Id = ?");
			// atribuir o valor de ?
			st.setInt(1, id);
			// obter o resultset atraves do executa a query
			rs = st.executeQuery();
			// se existir apenas 1 dado
			if (rs.next()) {
				// criar um departamento
				Department dep = instantiateDepartment(rs);
				// criar e retornar um seller
				Seller obj = instantiateSeller(rs, dep);
				// retorna o seller
				return obj;
			}
			return null;

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findAll() {
		// prepared statement
		PreparedStatement st = null;
		// result set
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					" SELECT seller.*,department.Name as DepName\r\n" + "FROM seller INNER JOIN department\r\n"
							+ "ON seller.DepartmentId = department.Id\r\n" + "ORDER BY Name");
			// obter o resultset atraves do executa a query
			rs = st.executeQuery();
			// uma lista de seller
			List<Seller> list = new ArrayList<>();
			// para evitar duplicação de novos objectos do departamento
			Map<Integer, Department> map = new HashMap<>();
			// por enquanto existir dados
			while (rs.next()) {
				// obter o departamento, se já existe
				Department dep = map.get(rs.getInt("DepartmentId"));
				// verificar se não existe
				if (dep == null) {
					// cria um novo departamento
					dep = instantiateDepartment(rs);
					// insira no map, para ver se existe
					map.put(rs.getInt("DepartmentId"), dep);
				}
				// criar e retornar um seller
				Seller seller = instantiateSeller(rs, dep);
				// adiciona o seller na lista
				list.add(seller);
			}
			// retorna a lista
			return list;

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		// prepared statement
		PreparedStatement st = null;
		// result set
		ResultSet rs = null;
		//
		try {
			st = conn.prepareStatement(
					" SELECT seller.*,department.Name as DepName " + "FROM seller INNER JOIN department "
							+ "ON seller.DepartmentId = department.Id " + "WHERE DepartmentId = ? " + "ORDER BY Name");
			// atribuir o valor de ?
			st.setInt(1, department.getId());
			// obter o resultset atraves do executa a query
			rs = st.executeQuery();
			// uma lista de seller
			List<Seller> list = new ArrayList<>();
			// para evitar duplicação de novos objectos do departamento
			Map<Integer, Department> map = new HashMap<>();
			// por enquanto existir dados
			while (rs.next()) {
				// obter o departamento, se já existe
				Department dep = map.get(rs.getInt("DepartmentId"));
				// verificar se não existe
				if (dep == null) {
					// cria um novo departamento
					dep = instantiateDepartment(rs);
					// insira no map, para ver se existe
					map.put(rs.getInt("DepartmentId"), dep);
				}
				// criar e retornar um seller
				Seller seller = instantiateSeller(rs, dep);
				// adiciona o seller na lista
				list.add(seller);
			}
			// retorna a lista
			return list;

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller obj = new Seller();
		obj.setId(rs.getInt("Id"));
		obj.setName(rs.getString("Name"));
		obj.setEmail(rs.getString("Email"));
		obj.setBaseSalary(rs.getDouble("BaseSalary"));
		obj.setBirthDate(rs.getDate("BirthDate"));
		obj.setDepartment(dep);
		return obj;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		return dep;
	}
}

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class FirebirdDataTransfer {

	public static void main(String[] args) {
		String urlOrigem = "jdbc:firebirdsql://127.0.0.1:3050/c:/firebird/banco.gdb";
		String urlDestino = "jdbc:firebirdsql://127.0.0.1:3050/c:/firebird/dados.fdb";
		String usuario = "sysdba";
		String senha = "masterkey";

		Connection conexaoOrigem = null;
		Connection conexaoDestino = null;

		try {
			// Conectar-se ao banco de dados de origem
			conexaoOrigem = DriverManager.getConnection(urlOrigem, usuario, senha);

			// Conectar-se ao banco de dados de destino
			conexaoDestino = DriverManager.getConnection(urlDestino, usuario, senha);

			// Selecionar os dados da tabela "produtos" do banco de dados de origem
			String sql = "SELECT PRO_CODIGO_BARRA, PRO_PRECO_VENDA,PRO_DESCRICAO FROM produtos ";
			Statement stmt = conexaoOrigem.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			System.out.println(rs);
			// Iterar pelos dados selecionados e inserir cada linha na tabela "produtos" do
			// banco de dados de destino
			Integer i=100;
			
			conexaoDestino.prepareStatement("delete from produto");
			
			while (rs.next()) {
				Integer CODIGO = i++;
				String TIPO = "00-MERCADORIA PARA REVENDA";
				String CODBARRA = rs.getString("PRO_CODIGO_BARRA");
				
				if(CODBARRA.length() == 14) {
					CODBARRA = rs.getString("PRO_CODIGO_BARRA").substring(1, 14);
				}
				Integer GRUPO = 1;
				String UNIDADE = "UN";
				double PR_VENDA = rs.getDouble("PRO_PRECO_VENDA");
				String NCM = "00000000";
				String ATIVO = "S";
				Integer EMPRESA = 1;
				String DESCRICAO = rs.getString("PRO_DESCRICAO");

				
				sql = "INSERT INTO produto (CODIGO, TIPO, CODBARRA,GRUPO,UNIDADE,PR_VENDA, NCM ,ATIVO,EMPRESA,DESCRICAO) VALUES (?, ?, ?, ? ,? ,? ,? ,?, ?,?)";
				System.out.println(sql);
				PreparedStatement pstmt = conexaoDestino.prepareStatement(sql);
				pstmt.setInt(1, CODIGO);
				pstmt.setString(2, TIPO);
				pstmt.setString(3, CODBARRA);
				pstmt.setInt(4, GRUPO);
				pstmt.setString(5, UNIDADE);
				pstmt.setDouble(6, PR_VENDA);
				pstmt.setString(7, NCM);
				pstmt.setString(8, ATIVO);
				pstmt.setInt(9, EMPRESA);
				pstmt.setString(10, DESCRICAO);
				try {
					pstmt.executeUpdate();
					
				}catch(SQLException e) {
					if(e.getErrorCode() == 335544665) {
						continue;
					}
				}

			}

			System.out.println("Transferência de dados concluída com sucesso.");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			// Fechar as conexões com os bancos de dados
			try {
				if (conexaoOrigem != null) {
					conexaoOrigem.close();
				}
				if (conexaoDestino != null) {
					conexaoDestino.close();
				}
			} catch (SQLException e) {
				System.out.println("Ocorreu um erro ao fechar a conexão com o banco de dados: " + e.getMessage());
			}
		}
	}
}

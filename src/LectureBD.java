import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;


public class LectureBD {   
	
	
	/* info pour le lab: 
	*
	*hostname : big-data-3.logti.etsmtl.ca
	*port : 1521
	*SID : LOG660
	*
	*User Passe : "equipe5","XCZDRZlk"
	 *
	 */
	
	//constante pour la connexion BD
	public static final String CONNECTION_BD ="";
	public static final String PILOTE_JDBC ="";
	Connection uneConnection;
	int count = 1;
	
   public class Role {
      public Role(int i, String n, String p) {
         id = i;
         nom = n;
         personnage = p;
      }
      protected int id;
      protected String nom;
      protected String personnage;
      
      public int getRoleId(){
    	  return id;
      }
   }
   
   public LectureBD() throws ClassNotFoundException, SQLException {
      connectionBD();                     
   }
   
   
   public void lecturePersonnes(String nomFichier) throws SQLException{   
	   int count = 0;
      try {
         XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
         XmlPullParser parser = factory.newPullParser();

         InputStream is = new FileInputStream(nomFichier);
         parser.setInput(is, null);

         int eventType = parser.getEventType();

         String tag = null, 
                nom = null,
                anniversaire = null,
                lieu = null,
                photo = null,
                bio = null;
         
         int id = -1;
         
         while (eventType != XmlPullParser.END_DOCUMENT) 
         { 
            if(eventType == XmlPullParser.START_TAG) 
            {
               tag = parser.getName();
               
               if (tag.equals("personne") && parser.getAttributeCount() == 1)
                  id = Integer.parseInt(parser.getAttributeValue(0));
            } 
            else if (eventType == XmlPullParser.END_TAG) 
            {                              
               tag = null;
               
               if (parser.getName().equals("personne") && id >= 0)
               {
                  insertionPersonne(id,nom,anniversaire,lieu,photo,bio);
                                    
                  id = -1;
                  nom = null;
                  anniversaire = null;
                  lieu = null;
                  photo = null;
                  bio = null;
 
                  
               }
            }
            else if (eventType == XmlPullParser.TEXT && id >= 0) 
            {
               if (tag != null)
               {                                    
                  if (tag.equals("nom"))
                     nom = parser.getText();
                  else if (tag.equals("anniversaire"))
                     anniversaire = parser.getText();
                  else if (tag.equals("lieu"))
                     lieu = parser.getText();
                  else if (tag.equals("photo"))
                     photo = parser.getText();
                  else if (tag.equals("bio"))
                     bio = parser.getText();
                  //test afficher les noms
                  
                /*  if (nom != null){
                      count = count+1;
                      System.out.println("nom: "+ nom + " anniversaire: "+anniversaire+ " lieu "+ lieu + " nombre: " +count) ;
                   }
               */
               }              
            }
          
            
            eventType = parser.next();            
        
         
         }
      }
      catch (XmlPullParserException e) {
          System.out.println(e);   
       }
       catch (IOException e) {
         System.out.println("IOException while parsing " + nomFichier); 
       }
      
      
   }   
   
   public void lectureFilms(String nomFichier) throws SQLException{
      try {
         XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
         XmlPullParser parser = factory.newPullParser();

         InputStream is = new FileInputStream(nomFichier);
         parser.setInput(is, null);

         int eventType = parser.getEventType();

         String tag = null, 
                titre = null,
                langue = null,
                poster = null,
                roleNom = null,
                rolePersonnage = null,
                realisateurNom = null,
                resume = null;
         
         ArrayList<String> pays = new ArrayList<String>();
         ArrayList<String> genres = new ArrayList<String>();
         ArrayList<String> scenaristes = new ArrayList<String>();
         ArrayList<Role> roles = new ArrayList<Role>();         
         ArrayList<String> annonces = new ArrayList<String>();
         
         int id = -1,
             annee = -1,
             duree = -1,
             roleId = -1,
             realisateurId = -1;
         
         while (eventType != XmlPullParser.END_DOCUMENT) 
         {
            if(eventType == XmlPullParser.START_TAG) 
            {
               tag = parser.getName();
               
               if (tag.equals("film") && parser.getAttributeCount() == 1)
                  id = Integer.parseInt(parser.getAttributeValue(0));
               else if (tag.equals("realisateur") && parser.getAttributeCount() == 1)
                  realisateurId = Integer.parseInt(parser.getAttributeValue(0));
               else if (tag.equals("acteur") && parser.getAttributeCount() == 1)
                  roleId = Integer.parseInt(parser.getAttributeValue(0));
            } 
            else if (eventType == XmlPullParser.END_TAG) 
            {                              
               tag = null;
               
               if (parser.getName().equals("film") && id >= 0)
               {
                  insertionFilm(id,titre,annee,pays,langue,
                             duree,resume,genres,realisateurNom,
                             realisateurId, scenaristes,
                             roles,poster,annonces);
                                    
                  id = -1;
                  annee = -1;
                  duree = -1;
                  titre = null;                                 
                  langue = null;                  
                  poster = null;
                  resume = null;
                  realisateurNom = null;
                  roleNom = null;
                  rolePersonnage = null;
                  realisateurId = -1;
                  roleId = -1;
                  
                  genres.clear();
                  scenaristes.clear();
                  roles.clear();
                  annonces.clear();  
                  pays.clear();
               }
               if (parser.getName().equals("role") && roleId >= 0) 
               {              
                  roles.add(new Role(roleId, roleNom, rolePersonnage));
                  roleId = -1;
                  roleNom = null;
                  rolePersonnage = null;
               }
            }
            else if (eventType == XmlPullParser.TEXT && id >= 0) 
            {
               if (tag != null)
               {                                    
                  if (tag.equals("titre"))
                     titre = parser.getText();
                  else if (tag.equals("annee"))
                     annee = Integer.parseInt(parser.getText());
                  else if (tag.equals("pays"))
                     pays.add(parser.getText());
                  else if (tag.equals("langue"))
                     langue = parser.getText();
                  else if (tag.equals("duree"))                 
                     duree = Integer.parseInt(parser.getText());
                  else if (tag.equals("resume"))                 
                     resume = parser.getText();
                  else if (tag.equals("genre"))
                     genres.add(parser.getText());
                  else if (tag.equals("realisateur"))
                     realisateurNom = parser.getText();
                  else if (tag.equals("scenariste"))
                     scenaristes.add(parser.getText());
                  else if (tag.equals("acteur"))
                     roleNom = parser.getText();
                  else if (tag.equals("personnage"))
                     rolePersonnage = parser.getText();
                  else if (tag.equals("poster"))
                     poster = parser.getText();
                  else if (tag.equals("annonce"))
                     annonces.add(parser.getText());                  
               }              
            }
            
            eventType = parser.next();            
         }
      }
      catch (XmlPullParserException e) {
          System.out.println(e);   
      }
      catch (IOException e) {
         System.out.println("IOException while parsing " + nomFichier); 
      }
   }
   
   public void lectureClients(String nomFichier) throws SQLException{
      try {
         XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
         XmlPullParser parser = factory.newPullParser();

         InputStream is = new FileInputStream(nomFichier);
         parser.setInput(is, null);

         int eventType = parser.getEventType();               

         String tag = null, 
                nomFamille = null,
                prenom = null,
                courriel = null,
                tel = null,
                anniv = null,
                adresse = null,
                ville = null,
                province = null,
                codePostal = null,
                carte = null,
                noCarte = null,
                motDePasse = null,
                forfait = null;                                 
         
         int id = -1,
             expMois = -1,
             expAnnee = -1;
         
         while (eventType != XmlPullParser.END_DOCUMENT) 
         {
            if(eventType == XmlPullParser.START_TAG) 
            {
               tag = parser.getName();
               
               if (tag.equals("client") && parser.getAttributeCount() == 1)
                  id = Integer.parseInt(parser.getAttributeValue(0));
            } 
            else if (eventType == XmlPullParser.END_TAG) 
            {                              
               tag = null;
               
               if (parser.getName().equals("client") && id >= 0)
               {
                  insertionClient(id,nomFamille,prenom,courriel,tel,
                             anniv,adresse,ville,province,
                             codePostal,carte,noCarte, 
                             expMois,expAnnee,motDePasse,forfait);               
                                    
                  nomFamille = null;
                  prenom = null;
                  courriel = null;               
                  tel = null;
                  anniv = null;
                  adresse = null;
                  ville = null;
                  province = null;
                  codePostal = null;
                  carte = null;
                  noCarte = null;
                  motDePasse = null; 
                  forfait = null;
                  
                  id = -1;
                  expMois = -1;
                  expAnnee = -1;
               }
            }
            else if (eventType == XmlPullParser.TEXT && id >= 0) 
            {         
               if (tag != null)
               {                                    
                  if (tag.equals("nom-famille"))
                     nomFamille = parser.getText();
                  else if (tag.equals("prenom"))
                     prenom = parser.getText();
                  else if (tag.equals("courriel"))
                     courriel = parser.getText();
                  else if (tag.equals("tel"))
                     tel = parser.getText();
                  else if (tag.equals("anniversaire"))
                     anniv = parser.getText();
                  else if (tag.equals("adresse"))
                     adresse = parser.getText();
                  else if (tag.equals("ville"))
                     ville = parser.getText();
                  else if (tag.equals("province"))
                     province = parser.getText();
                  else if (tag.equals("code-postal"))
                     codePostal = parser.getText();
                  else if (tag.equals("carte"))
                     carte = parser.getText();
                  else if (tag.equals("no"))
                     noCarte = parser.getText();
                  else if (tag.equals("exp-mois"))                 
                     expMois = Integer.parseInt(parser.getText());
                  else if (tag.equals("exp-annee"))                 
                     expAnnee = Integer.parseInt(parser.getText());
                  else if (tag.equals("mot-de-passe"))                 
                     motDePasse = parser.getText();  
                  else if (tag.equals("forfait"))                 
                     forfait = parser.getText(); 
               }              
            }
            
            eventType = parser.next();            
         }
      }
      catch (XmlPullParserException e) {
          System.out.println(e);   
      }
      catch (IOException e) {
         System.out.println("IOException while parsing " + nomFichier); 
      }
   }   
   
   private void insertionPersonne(int id, String nom, String anniv, String lieu, String photo, String bio) throws SQLException {      
      // On insere la personne dans la BD
	   
	   
	   ResultSet rs;
	   String ville = "";
	   String province = "";
	   String prenom = "";
	   String pays = "";
	   int idLieu =0;
	   int idCelebrite =0;
	   int idPersonne = 0;
	   
	   // System.out.println("id : "+ id +"compteur : " + count++ );
	   if(uneConnection == null){
		   System.out.println("il n'y a pas de connection");
	   }else{
		   String espace = " ";
		   String[] tokens = nom.split(espace);
		   prenom = tokens[0];
		   for (int i=1; i<tokens.length; i++){
		   nom = nom + tokens[i]+" ";
		   }
		   String virgule= ",";
		   String[] tokensLieu = lieu.split(virgule);
		   ville = tokensLieu[0];
		   province = tokensLieu[1];
		   pays = tokensLieu[2];
		   
		   //insertion dans la table personne, retour de idPersonne
		   PreparedStatement insertionPersonne = 
				   uneConnection.prepareStatement("INSERT INTO Personne("
				   		+ "nom, prenom,dateNaissance) VALUES((?,?,?)", new String[]{"idPersonne"} );
		   insertionPersonne.setString(1, nom);
		   insertionPersonne.setString(2, prenom);
		   insertionPersonne.setString(3, anniv);
		   insertionPersonne.executeUpdate();
		   rs = insertionPersonne.getGeneratedKeys();
		   if(rs.next()){
			   idPersonne = rs.getInt(1);
		   }
		   
		   
		   //insertion dans la table Lieu, retour de idLieu
		   PreparedStatement insertionLieu = 
				   uneConnection.prepareStatement("INSERT INTO Lieu(ville, province, idPays) VALUES((?,?,?)", new String[]{"idLieu"} );
		   insertionLieu.setString(1, ville);
		   insertionLieu.setString(2, province);
		  // insertionLieu.setString(3, pays);
		   insertionLieu.executeUpdate();
		   rs = insertionLieu.getGeneratedKeys();
		   if(rs.next()){
			   idLieu= rs.getInt(1);
		   }
	   
		   //insertion dans la table Celebrite, retour de celebrite
		   PreparedStatement insertionCelebrite = 
				   uneConnection.prepareStatement("INSERT INTO Personne("
				   		+ "idPersonne, biographie,idLieu) VALUES((?,?,?)", new String[]{"idPersonne"} );
		   insertionCelebrite.setInt(1, idPersonne);
		   insertionCelebrite.setString(2, bio);
		   insertionCelebrite.setInt(3, idLieu);
		   insertionCelebrite.executeUpdate();
		   rs = insertionCelebrite.getGeneratedKeys();
		   if(rs.next()){
			   idCelebrite = rs.getInt(1);
		   }
		   
	   }
   }
   
   private void insertionFilm(int id, String titre, int annee,
                           ArrayList<String> pays, String langue, int duree, String resume,
                           ArrayList<String> genres, String realisateurNom, int realisateurId,
                           ArrayList<String> scenaristes,
                           ArrayList<Role> roles, String poster,
                           ArrayList<String> annonces) throws SQLException {         
      // On le film dans la BD
	   
		/*
		 * ***test***
	   for (int i =0; i<roles.size();i++){
	   System.out.println("role : "+ roles.get(i).getRoleId() +"compteur : " + count++ );
	   }
	   */
	   
	   ResultSet rs;
	   int idFilm = 0;
	   if(uneConnection == null){
		   System.out.println("il n'y a pas de connection");
	   }else{
	   
	   //insertion dans la table personne, retour de idPersonne
	   PreparedStatement insertionFilm = 
			   uneConnection.prepareStatement("INSERT INTO Film("
			   		+ "titre, anneeSortie, duree, resumeScenario) VALUES((?,?,?,?)", new String[]{"idFilm"} );
	   insertionFilm.setString(1, titre);
	   insertionFilm.setInt(2, annee);
	   insertionFilm.setInt(3, duree);
	   insertionFilm.setString(4, resume);
	   insertionFilm.executeUpdate();
	   rs = insertionFilm.getGeneratedKeys();
	   if(rs.next()){
		   idFilm = rs.getInt(1);
	   }
	   
	   //*************A implementer: créer la table Genre pour référence 
	   //insertion dans la table personne, retour de idPersonne
	   //changer le ID genre devrait devenir un type... action, aventure etc
	   
	   for(int i=0; i<genres.size();i++){
	   PreparedStatement insertionGenreFilm = 
			   uneConnection.prepareStatement("INSERT INTO GenreFilm("
			   		+ "idFilm, idGenre) VALUES((?,?)" );
	   insertionGenreFilm.setInt(1, idFilm);
	   insertionGenreFilm.setString(2, genres.get(i));
	  
	   insertionGenreFilm.executeUpdate();
	   }
	   
	   for(int i=0; i<pays.size();i++){
		   PreparedStatement insertionPaysFilm = 
				   uneConnection.prepareStatement("INSERT INTO PaysFilm("
				   		+ "idFilm, idPays) VALUES((?,?)" );
		   insertionPaysFilm.setInt(1, idFilm);
		   insertionPaysFilm.setString(2, pays.get(i));
		  
		   insertionPaysFilm.executeUpdate();
		   }
	   
	   for(int i=0; i<scenaristes.size();i++){
		   PreparedStatement insertionScenaristeFilm = 
				   uneConnection.prepareStatement("INSERT INTO ScenaristeFilm("
				   		+ "idFilm, nom ) VALUES((?,?)" );
		   insertionScenaristeFilm.setInt(1, idFilm);
		   insertionScenaristeFilm.setString(2, scenaristes.get(i));
		  
		   insertionScenaristeFilm.executeUpdate();
		   }
	   
	   
	   }
	   
   }
   
  
   private void insertionClient(int id, String nomFamille, String prenom,
                             String courriel, String tel, String anniv,
                             String adresse, String ville, String province,
                             String codePostal, String carte, String noCarte,
                             int expMois, int expAnnee, String motDePasse,
                             String forfait) throws SQLException {
      // On le client dans la BD
	   
	   int idPersonne = 0;
	   int idUtilisateur = 0;
	   int idAdresse = 0;
	   int idLieu = 0;
	   int idPays = 1;
	   int idClient = 1;
	   
	   String noCivique = "";
	   String rue = "";
	   ResultSet rs;
	   String espace = " ";
	   String[] tokens = adresse.split(espace);
	   noCivique = tokens[0];
	   for (int i=1; i<tokens.length; i++){
	   rue = rue +tokens[i]+" ";
	   }
	  // System.out.println("id : "+ id +"conteur : " + count++ );
	   if(uneConnection == null){
		   System.out.println("il n'y a pas de connection");
	   }else{
		   
		   //insertion dans la table personne, retour de idPersonne
		   PreparedStatement insertionPersonne = 
				   uneConnection.prepareStatement("INSERT INTO Personne("
				   		+ "nom, prenom,dateNaissance) VALUES((?,?,?)", new String[]{"idPersonne"} );
		   insertionPersonne.setString(1, nomFamille);
		   insertionPersonne.setString(2, prenom);
		   insertionPersonne.setString(3, anniv);
		   insertionPersonne.executeUpdate();
		   rs = insertionPersonne.getGeneratedKeys();
		   if(rs.next()){
			   idPersonne = rs.getInt(1);
		   }
		   
		   //insertion dans la table Lieu, retour de idLieu
		   PreparedStatement insertionLieu = 
				   uneConnection.prepareStatement("INSERT INTO Lieu(ville, province, idPays) VALUES((?,?,?)", new String[]{"idLieu"} );
		   insertionLieu.setString(1, ville);
		   insertionLieu.setString(2, province);
		   insertionLieu.setInt(3, idPays);
		   insertionLieu.executeUpdate();
		   rs = insertionLieu.getGeneratedKeys();
		   if(rs.next()){
			   idLieu= rs.getInt(1);
		   }
			   //insertion dans la table adresse, retour de idAdresse
		   PreparedStatement insertionAdresse = 
				   uneConnection.prepareStatement("INSERT INTO Adresse(noCivique, rue, codePostal, idLieu) VALUES((?,?,?,?)", new String[]{"idAdresse"} );
		   insertionAdresse.setString(1, noCivique);
		   insertionAdresse.setString(2, rue);
		   insertionAdresse.setString(3, codePostal);
		   insertionAdresse.setInt(4, idLieu);
		   insertionAdresse.executeUpdate();
		   rs = insertionAdresse.getGeneratedKeys();
		   if(rs.next()){
			   idAdresse= rs.getInt(1);

		   }
		   
		    // insertion dans la table Utilisateur, retour idUtilisateur
		   PreparedStatement insertionUtilisateur = 
				   uneConnection.prepareStatement("INSERT INTO Utilisateur(idPersonne, motDePasse, courriel, noTelephone, idAdresse) VALUES((?,?,?,?,?)", new String[]{"idUtilisateur"} );
		   insertionUtilisateur.setInt(1, idPersonne);
		   insertionUtilisateur.setString(2, motDePasse);
		   insertionUtilisateur.setString(3, courriel);
		   insertionUtilisateur.setString(4, tel);
		   insertionUtilisateur.setInt(5, idAdresse);
		   insertionUtilisateur.executeUpdate();
		   rs = insertionUtilisateur.getGeneratedKeys();
		   if(rs.next()){
			   idUtilisateur = rs.getInt(1);
		   }
		   
		// insertion dans la table Carte de credit
		   PreparedStatement insertionCarteCredit = 
				   uneConnection.prepareStatement("INSERT INTO CarteCredit(numero,idClient, type, moisExpiration, anneeExpiration) VALUES((?,?,?,?,?)" );
		   insertionCarteCredit.setString(1, noCarte);
		   insertionCarteCredit.setInt(2, idClient);
		   insertionCarteCredit.setString(3, carte);
		   insertionCarteCredit.setInt(4, expMois);
		   insertionCarteCredit.setInt(5, expAnnee);
		   insertionCarteCredit.executeUpdate();
		   
		   }
	// insertion dans la table Client
	   PreparedStatement insertionClient = 
			   uneConnection.prepareStatement("INSERT INTO Client(idUtilisateur, idForfait) VALUES((?,?)", new String[]{"idClient"} );
	   insertionClient.setInt(1, idUtilisateur);
	   insertionClient.setString(2, forfait);
	  
	   insertionClient.executeUpdate();
	 
	   rs = insertionClient.getGeneratedKeys();
	   if(rs.next()){
		   idClient = rs.getInt(1);
	   }
}
   
   
   private void connectionBD() throws ClassNotFoundException, SQLException {
      // On se connecte a la BD
	  
	   // charger le pilote JDBC
//		Class.forName(PILOTE_JDBC);
		
		//connection a la BD
//		uneConnection = DriverManager.getConnection(CONNECTION_BD);
	

	   
   }

   public static void main(String[] args) throws ClassNotFoundException, SQLException {
      LectureBD lecture = new LectureBD();
      
    //  lecture.lecturePersonnes(args[0]);
      lecture.lectureFilms(args[1]);
    //  lecture.lectureClients(args[2]);
   }
}

package com.tom.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * ClassName: Test01
 * Description:
 *
 * @author Mi_dad
 * @date 2020/1/4 10:37
 */
public class Test01 {
    public static void main(String[] args) throws IOException {
        File file = new File("D:\\z_test\\message.in");
        FileInputStream fs = new FileInputStream(file);
        Integer line = null;
        while ((line = fs.read()) !=null){

        }
    }
//    public static int getMax(){
//        freopen("message.in","r",stdin);
//        freopen("message.ans","w",stdout);
//
//
//        std::cin>>n>>m;
//
//        for(int i=1;i<=n;i++)
//            for(int j=1;j<=m;j++)
//                std::cin>>s[i][j];
//        for(int i=3;i<=m+n;i++)
//            for(int j=1;j<=std::min(i-1,n);j++)
//        for(int k=1;k<=std::min(i-1,n);k++){
//            if(i<m+n&&j==k)continue;
//            dp[i][j][k]=std::max(dp[i-1][j][k],dp[i-1][j-1][k-1]);
//            if(j-1!=k)dp[i][j][k]=std::max(dp[i][j][k],dp[i-1][j-1][k]);
//            if(k-1!=j)dp[i][j][k]=std::max(dp[i][j][k],dp[i-1][j][k-1]);
//            dp[i][j][k]+=s[j][i-j]+s[k][i-k];
//        }
//        std::cout<<dp[m+n][n][n]<<std::endl;
//        return 0;
//    }
}
